defmodule NimblePoolTest do
  use ExUnit.Case

  defmodule StatelessPool do
    @behaviour NimblePool

    def init(instructions) do
      next(instructions, :init, [])
    end

    def handle_checkout(from, instructions) do
      next(instructions, :handle_checkout, [from])
    end

    def handle_checkin(client_state, from, instructions) do
      next(instructions, :handle_checkin, [client_state, from])
    end

    def handle_info(message, instructions) do
      next(instructions, :handle_info, [message])
    end

    def terminate(reason, instructions) do
      # We always allow skip ahead on terminate
      instructions = Enum.drop_while(instructions, &(elem(&1, 0) != :terminate))
      next(instructions, :terminate, [reason])
    end

    defp next([{instruction, return} | instructions], instruction, args) do
      apply(return, args ++ [instructions])
    end

    defp next(instructions, instruction, _args) do
      raise "expected #{inspect(instruction)}, state was #{inspect(instructions)}"
    end
  end

  defmodule TestAgent do
    use Agent

    def start_link(instructions) do
      Agent.start_link(fn -> instructions end)
    end

    def next(pid, instruction, args) do
      Agent.get_and_update(pid, fn
        [{^instruction, return} | instructions] when is_function(return) ->
          {apply(return, args), instructions}

        state ->
          raise "expected #{inspect(instruction)}, state was #{inspect(state)}"
      end)
    end

    def instructions(pid) do
      Agent.get(pid, & &1)
    end
  end

  defmodule StatefulPool do
    @behaviour NimblePool

    def init(pid) do
      TestAgent.next(pid, :init, [pid])
    end

    def handle_checkout(from, pid) do
      TestAgent.next(pid, :handle_checkout, [from, pid])
    end

    def handle_checkin(client_state, from, pid) do
      TestAgent.next(pid, :handle_checkin, [client_state, from, pid])
    end

    def handle_info(message, pid) do
      TestAgent.next(pid, :handle_info, [message, pid])
    end

    def terminate(reason, pid) do
      TestAgent.next(pid, :terminate, [reason, pid])
    end
  end

  defp stateless_pool!(instructions, opts \\ []) do
    start_pool!(StatelessPool, instructions, opts)
  end

  defp stateful_pool!(instructions, opts \\ []) do
    {:ok, agent} = TestAgent.start_link(instructions)
    {agent, start_pool!(StatefulPool, agent, opts)}
  end

  defp start_pool!(worker, arg, opts) do
    opts = Keyword.put_new(opts, :pool_size, 1)

    start_supervised!(
      {NimblePool, [worker: {worker, arg}] ++ opts},
      restart: :temporary
    )
  end

  defp assert_drained(agent) do
    case TestAgent.instructions(agent) do
      [] -> :ok
      instructions -> flunk("pending instructions found: #{inspect(instructions)}")
    end
  end

  test "starts the pool with checkout, checkin, and terminate" do
    parent = self()

    pool =
      stateless_pool!(
        init: fn next -> {:ok, next} end,
        handle_checkout: fn _from, next -> {:ok, :client_state_out, next} end,
        handle_checkin: fn :client_state_in, _from, next -> {:ok, next} end,
        terminate: fn reason, [] -> send(parent, {:terminate, reason}) end
      )

    assert NimblePool.checkout!(pool, fn :client_state_out ->
             {:result, :client_state_in}
           end) == :result

    NimblePool.stop(pool, :shutdown)
    assert_receive {:terminate, :shutdown}

    # Assert down from checkout! did not leak
    refute_received {:DOWN, _, _, _, _}
  end

  describe "checkout!" do
    test "exits with noproc for missing process" do
      assert catch_exit(NimblePool.checkout!(:unknown, &{&1, &1})) ==
               {:noproc, {NimblePool, :checkout, [:unknown]}}
    end

    test "exits when pool terminates on checkout" do
      Process.flag(:trap_exit, true)

      pool =
        stateless_pool!(
          init: fn next -> {:ok, next} end,
          handle_checkout: fn _from, _next -> Process.exit(self(), :kill) end
        )

      assert catch_exit(NimblePool.checkout!(pool, &{&1, &1})) ==
               {:killed, {NimblePool, :checkout, [pool]}}
    end

    test "exits when pool terminates on checkin" do
      Process.flag(:trap_exit, true)

      pool =
        stateless_pool!(
          init: fn next -> {:ok, next} end,
          handle_checkout: fn _from, next -> {:ok, :client_state_out, next} end,
          handle_checkin: fn :client_state_in, _from, _next -> Process.exit(self(), :kill) end
        )

      assert catch_exit(
               NimblePool.checkout!(pool, fn :client_state_out -> {:result, :client_state_in} end)
             ) ==
               {:killed, {NimblePool, :checkin, [pool]}}
    end

    test "restarts worker on client throw/error/exit during checkout" do
      parent = self()

      pool =
        stateless_pool!(
          init: fn next -> send(parent, :started) && {:ok, next} end,
          handle_checkout: fn _from, next -> {:ok, :client_state_out, next} end,
          handle_checkin: fn :client_state_in, _from, next -> {:ok, next} end,
          terminate: fn reason, _ -> send(parent, {:terminate, reason}) end
        )

      # Started once
      assert_receive :started
      refute_received :started

      assert_raise RuntimeError, "oops", fn ->
        NimblePool.checkout!(pool, fn :client_state_out -> raise "oops" end)
      end

      # Terminated and restarted
      assert_receive {:terminate, :error}
      assert_receive :started
      refute_received :started

      # Do a proper checkout now
      assert NimblePool.checkout!(pool, fn :client_state_out ->
               {:result, :client_state_in}
             end) == :result

      # Assert down from failed checkout! did not leak
      NimblePool.stop(pool, :shutdown)
      refute_received {:DOWN, _, _, _, _}
      assert_receive {:terminate, :shutdown}
    end

    test "restarts worker on client down during checkout" do
      parent = self()

      pool =
        stateless_pool!(
          init: fn next -> send(parent, :started) && {:ok, next} end,
          handle_checkout: fn _from, next -> {:ok, :client_state_out, next} end,
          handle_checkin: fn :client_state_in, _from, next -> {:ok, next} end,
          terminate: fn reason, _ -> send(parent, {:terminate, reason}) end
        )

      # Started once
      assert_receive :started
      refute_received :started

      {:ok, pid} =
        Task.start(fn ->
          NimblePool.checkout!(pool, fn :client_state_out ->
            send(parent, :lock)
            Process.sleep(:infinity)
          end)
        end)

      # Once it is locked we can kill it
      assert_receive :lock
      Process.exit(pid, :you_shall_not_pass)

      # Terminated and restarted
      assert_receive {:terminate, :DOWN}
      assert_receive :started
      refute_received :started

      # Do a proper checkout now
      assert NimblePool.checkout!(pool, fn :client_state_out ->
               {:result, :client_state_in}
             end) == :result

      # Assert down from failed checkout! did not leak
      NimblePool.stop(pool, :shutdown)
      refute_received {:DOWN, _, _, _, _}
      assert_receive {:terminate, :shutdown}
    end

    test "restarts worker on client timeout during checkout" do
      parent = self()

      pool =
        stateless_pool!(
          init: fn next -> send(parent, :started) && {:ok, next} end,
          handle_checkout: fn _from, next -> {:ok, :client_state_out, next} end,
          handle_checkin: fn :client_state_in, _from, next -> {:ok, next} end,
          terminate: fn reason, _ -> send(parent, {:terminate, reason}) end
        )

      # Started once
      assert_receive :started
      refute_received :started

      # Suspend the pool, this will trigger a timeout but the message will be delivered
      :sys.suspend(pool)

      assert catch_exit(NimblePool.checkout!(pool, fn _ -> raise "never invoked" end, 0)) ==
               {:timeout, {NimblePool, :checkout, [pool]}}

      :sys.resume(pool)

      # Terminated and restarted
      assert_receive {:terminate, :timeout}
      assert_receive :started
      refute_received :started

      # Do a proper checkout now
      assert NimblePool.checkout!(pool, fn :client_state_out ->
               {:result, :client_state_in}
             end) == :result

      # Assert down from failed checkout! did not leak
      NimblePool.stop(pool, :shutdown)
      refute_received {:DOWN, _, _, _, _}
      assert_receive {:terminate, :shutdown}
    end

    test "does not restart worker on client timeout during checkin" do
      parent = self()

      pool =
        stateless_pool!(
          init: fn next -> send(parent, :started) && {:ok, next} end,
          handle_checkout: fn _from, next -> {:ok, :client_state_out, next} end,
          handle_checkin: fn :client_state_in, _from, next -> {:ok, next} end,
          handle_checkout: fn _from, next -> {:ok, :client_state_out2, next} end,
          handle_checkin: fn :client_state_in2, _from, next -> {:ok, next} end,
          terminate: fn reason, _ -> send(parent, {:terminate, reason}) end
        )

      # Started once
      assert_receive :started
      refute_received :started

      assert catch_exit(
               NimblePool.checkout!(
                 pool,
                 fn :client_state_out ->
                   # Suspend the pool, this will trigger a timeout but the message will be delivered
                   :sys.suspend(pool)
                   {:ok, :client_state_in}
                 end,
                 100
               )
             ) == {:timeout, {NimblePool, :checkin, [pool]}}

      :sys.resume(pool)

      # Terminated and restarted
      refute_received {:terminate, _}
      refute_received :started

      # Do a proper checkout now
      assert NimblePool.checkout!(pool, fn :client_state_out2 ->
               {:result, :client_state_in2}
             end) == :result

      # Assert down from failed checkin! did not leak
      NimblePool.stop(pool, :shutdown)
      refute_received {:DOWN, _, _, _, _}
      assert_receive {:terminate, :shutdown}
    end

    test "does not restart worker on client timeout during unused checkout" do
      parent = self()

      pool =
        stateless_pool!(
          init: fn next -> send(parent, :started) && {:ok, next} end,
          handle_checkout: fn _from, next -> {:ok, :client_state_out, next} end,
          handle_checkin: fn :client_state_in, _from, next -> {:ok, next} end,
          handle_checkout: fn _from, next -> {:ok, :client_state_out2, next} end,
          handle_checkin: fn :client_state_in2, _from, next -> {:ok, next} end,
          terminate: fn reason, _ -> send(parent, {:terminate, reason}) end
        )

      # Started once
      assert_receive :started
      refute_received :started

      # Checkout the pool in a separate process
      task =
        Task.async(fn ->
          NimblePool.checkout!(pool, fn :client_state_out ->
            send(parent, :lock)
            assert_receive :release
            {:result, :client_state_in}
          end)
        end)

      assert_receive :lock

      # Now we do a failed checkout
      assert catch_exit(NimblePool.checkout!(pool, fn _ -> raise "never invoked" end, 0)) ==
               {:timeout, {NimblePool, :checkout, [pool]}}

      # And we release the original checkout
      send(task.pid, :release)
      assert Task.await(task) == :result

      # Terminated and restarted
      refute_received {:terminate, :timeout}
      refute_received :started

      # Do a proper checkout now and it still works
      assert NimblePool.checkout!(pool, fn :client_state_out2 ->
               {:result, :client_state_in2}
             end) == :result
    end

    test "queues checkouts" do
      parent = self()

      pool =
        stateless_pool!(
          init: fn next -> {:ok, next} end,
          handle_checkout: fn _from, next -> {:ok, :client_state_out, next} end,
          handle_checkin: fn :client_state_in, _from, next -> {:ok, next} end,
          handle_checkout: fn _from, next -> {:ok, :client_state_out2, next} end,
          handle_checkin: fn :client_state_in2, _from, next -> {:ok, next} end,
          terminate: fn _, _ -> :ok end
        )

      # Checkout the pool in a separate process
      task1 =
        Task.async(fn ->
          NimblePool.checkout!(pool, fn :client_state_out ->
            send(parent, :lock)
            assert_receive :release
            {:result, :client_state_in}
          end)
        end)

      assert_receive :lock

      # Checkout the pool in another process
      task2 =
        Task.async(fn ->
          NimblePool.checkout!(pool, fn :client_state_out2 ->
            {:result, :client_state_in2}
          end)
        end)

      # Release them
      send(task1.pid, :release)
      assert Task.await(task1) == :result
      assert Task.await(task2) == :result
    end
  end
end
