defmodule NimblePoolTest do
  use ExUnit.Case

  defmodule StatelessPool do
    @behaviour NimblePool

    def init_pool({_worker, arg, _pool_size} = init_args) do
      if is_function(arg[:init_pool]) do
        arg[:init_pool].(init_args)
      else
        :ok
      end
    end

    def init(instructions) do
      instructions
      |> Keyword.delete(:init_pool)
      |> next(:init, [])
    end

    def handle_checkout(command, from, instructions) do
      next(instructions, :handle_checkout, [command, from])
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
      return =
        Agent.get_and_update(pid, fn
          [{^instruction, return} | instructions] when is_function(return) ->
            {return, instructions}

          state ->
            raise "expected #{inspect(instruction)}, state was #{inspect(state)}"
        end)

      apply(return, args)
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

    def handle_checkout(command, from, pid) do
      TestAgent.next(pid, :handle_checkout, [command, from, pid])
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

  test "starts the pool with init_pool, checkout, checkin, and terminate" do
    parent = self()

    pool =
      stateless_pool!(
        init_pool: fn _ ->
          send(parent, :init_pool)
          :ok
        end,
        init: fn next -> {:ok, next} end,
        handle_checkout: fn :checkout, _from, next -> {:ok, :client_state_out, next} end,
        handle_checkin: fn :client_state_in, _from, next -> {:ok, next} end,
        terminate: fn reason, [] -> send(parent, {:terminate, reason}) end
      )

    assert {:messages, [:init_pool | _]} = Process.info(self(), :messages)
    assert_receive :init_pool

    assert NimblePool.checkout!(pool, :checkout, fn :client_state_out ->
             {:result, :client_state_in}
           end) == :result

    NimblePool.stop(pool, :shutdown)
    assert_receive {:terminate, :shutdown}

    # Assert down from checkout! did not leak
    refute_received {:DOWN, _, _, _, _}
  end

  describe "init" do
    test "raises on invalid return" do
      assert_raise RuntimeError, ~r"Expected: {:ok, state} | {:async, (() -> state)}", fn ->
        stateless_pool!(init: fn _ -> :oops end)
      end
    end
  end

  describe "checkout!" do
    test "exits with noproc for missing process" do
      assert catch_exit(NimblePool.checkout!(:unknown, :checkout, &{&1, &1})) ==
               {:noproc, {NimblePool, :checkout, [:unknown]}}
    end

    test "exits when pool terminates on checkout" do
      Process.flag(:trap_exit, true)

      pool =
        stateless_pool!(
          init: fn next -> {:ok, next} end,
          handle_checkout: fn :checkout, _from, _next -> Process.exit(self(), :kill) end
        )

      assert catch_exit(NimblePool.checkout!(pool, :checkout, &{&1, &1})) ==
               {:killed, {NimblePool, :checkout, [pool]}}
    end

    test "exits when pool terminates on checkin" do
      Process.flag(:trap_exit, true)

      pool =
        stateless_pool!(
          init: fn next -> {:ok, next} end,
          handle_checkout: fn :checkout, _from, next -> {:ok, :client_state_out, next} end,
          handle_checkin: fn :client_state_in, _from, _next -> Process.exit(self(), :kill) end
        )

      assert catch_exit(
               NimblePool.checkout!(pool, :checkout, fn :client_state_out ->
                 {:result, :client_state_in}
               end)
             ) ==
               {:killed, {NimblePool, :checkin, [pool]}}
    end

    test "restarts worker on client throw/error/exit during checkout" do
      parent = self()

      pool =
        stateless_pool!(
          init: fn next -> send(parent, :started) && {:ok, next} end,
          handle_checkout: fn :checkout, _from, next -> {:ok, :client_state_out, next} end,
          handle_checkin: fn :client_state_in, _from, next -> {:ok, next} end,
          terminate: fn reason, _ -> send(parent, {:terminate, reason}) end
        )

      # Started once
      assert_receive :started
      refute_received :started

      assert_raise RuntimeError, "oops", fn ->
        NimblePool.checkout!(pool, :checkout, fn :client_state_out -> raise "oops" end)
      end

      # Terminated and restarted
      assert_receive {:terminate, :error}
      assert_receive :started
      refute_received :started

      # Do a proper checkout now
      assert NimblePool.checkout!(pool, :checkout, fn :client_state_out ->
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
          handle_checkout: fn :checkout, _from, next -> {:ok, :client_state_out, next} end,
          handle_checkin: fn :client_state_in, _from, next -> {:ok, next} end,
          terminate: fn reason, _ -> send(parent, {:terminate, reason}) end
        )

      # Started once
      assert_receive :started
      refute_received :started

      {:ok, pid} =
        Task.start(fn ->
          NimblePool.checkout!(pool, :checkout, fn :client_state_out ->
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
      assert NimblePool.checkout!(pool, :checkout, fn :client_state_out ->
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
          handle_checkout: fn :checkout, _from, next -> {:ok, :client_state_out, next} end,
          handle_checkin: fn :client_state_in, _from, next -> {:ok, next} end,
          terminate: fn reason, _ -> send(parent, {:terminate, reason}) end
        )

      # Started once
      assert_receive :started
      refute_received :started

      # Suspend the pool, this will trigger a timeout but the message will be delivered
      :sys.suspend(pool)

      assert catch_exit(
               NimblePool.checkout!(pool, :checkout, fn _ -> raise "never invoked" end, 0)
             ) ==
               {:timeout, {NimblePool, :checkout, [pool]}}

      :sys.resume(pool)

      # Terminated and restarted
      assert_receive {:terminate, :timeout}
      assert_receive :started
      refute_received :started

      # Do a proper checkout now
      assert NimblePool.checkout!(pool, :checkout, fn :client_state_out ->
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
          handle_checkout: fn :checkout, _from, next -> {:ok, :client_state_out, next} end,
          handle_checkin: fn :client_state_in, _from, next -> {:ok, next} end,
          handle_checkout: fn :checkout2, _from, next -> {:ok, :client_state_out2, next} end,
          handle_checkin: fn :client_state_in2, _from, next -> {:ok, next} end,
          terminate: fn reason, _ -> send(parent, {:terminate, reason}) end
        )

      # Started once
      assert_receive :started
      refute_received :started

      assert catch_exit(
               NimblePool.checkout!(
                 pool,
                 :checkout,
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
      assert NimblePool.checkout!(pool, :checkout2, fn :client_state_out2 ->
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
          handle_checkout: fn :checkout, _from, next -> {:ok, :client_state_out, next} end,
          handle_checkin: fn :client_state_in, _from, next -> {:ok, next} end,
          handle_checkout: fn :checkout2, _from, next -> {:ok, :client_state_out2, next} end,
          handle_checkin: fn :client_state_in2, _from, next -> {:ok, next} end,
          terminate: fn reason, _ -> send(parent, {:terminate, reason}) end
        )

      # Started once
      assert_receive :started
      refute_received :started

      # Checkout the pool in a separate process
      task =
        Task.async(fn ->
          NimblePool.checkout!(pool, :checkout, fn :client_state_out ->
            send(parent, :lock)
            assert_receive :release
            {:result, :client_state_in}
          end)
        end)

      assert_receive :lock

      # Now we do a failed checkout
      assert catch_exit(
               NimblePool.checkout!(pool, :checkout, fn _ -> raise "never invoked" end, 0)
             ) ==
               {:timeout, {NimblePool, :checkout, [pool]}}

      # And we release the original checkout
      send(task.pid, :release)
      assert Task.await(task) == :result

      # Terminated and restarted
      refute_received {:terminate, :timeout}
      refute_received :started

      # Do a proper checkout now and it still works
      assert NimblePool.checkout!(pool, :checkout2, fn :client_state_out2 ->
               {:result, :client_state_in2}
             end) == :result
    end

    test "queues checkouts" do
      parent = self()

      pool =
        stateless_pool!(
          init: fn next -> {:ok, next} end,
          handle_checkout: fn :checkout, _from, next -> {:ok, :client_state_out, next} end,
          handle_checkin: fn :client_state_in, _from, next -> {:ok, next} end,
          handle_checkout: fn :checkout2, _from, next -> {:ok, :client_state_out2, next} end,
          handle_checkin: fn :client_state_in2, _from, next -> {:ok, next} end,
          terminate: fn _, _ -> :ok end
        )

      # Checkout the pool in a separate process
      task1 =
        Task.async(fn ->
          NimblePool.checkout!(pool, :checkout, fn :client_state_out ->
            send(parent, :lock)
            assert_receive :release
            {:result, :client_state_in}
          end)
        end)

      assert_receive :lock

      # Checkout the pool in another process
      task2 =
        Task.async(fn ->
          NimblePool.checkout!(pool, :checkout2, fn :client_state_out2 ->
            {:result, :client_state_in2}
          end)
        end)

      # Release them
      send(task1.pid, :release)
      assert Task.await(task1) == :result
      assert Task.await(task2) == :result
    end

    test "concurrent checkouts" do
      parent = self()

      pool =
        stateless_pool!(
          [
            init: fn next -> {:ok, next} end,
            handle_checkout: fn :checkout, _from, next -> {:ok, :client_state_out, next} end,
            handle_checkin: fn :client_state_in, _from, next -> {:ok, next} end,
            terminate: fn _, _ -> :ok end
          ],
          pool_size: 2
        )

      task1 =
        Task.async(fn ->
          NimblePool.checkout!(pool, :checkout, fn :client_state_out ->
            send(parent, :lock)
            assert_receive :release
            {:result, :client_state_in}
          end)
        end)

      assert_receive :lock

      task2 =
        Task.async(fn ->
          NimblePool.checkout!(pool, :checkout, fn :client_state_out ->
            send(parent, :lock)
            assert_receive :release
            {:result, :client_state_in}
          end)
        end)

      assert_receive :lock

      send(task1.pid, :release)
      send(task2.pid, :release)
      assert Task.await(task1) == :result
      assert Task.await(task2) == :result
    end
  end

  describe "handle_info" do
    test "is invoked for all children" do
      parent = self()

      pool =
        stateless_pool!(
          [
            init: fn next -> {:ok, next} end,
            handle_info: fn :handle_info, next -> send(parent, :info) && {:ok, next} end,
            terminate: fn _, _ -> :ok end
          ],
          pool_size: 2
        )

      send(pool, :handle_info)

      assert_receive :info
      assert_receive :info
    end

    test "forwards DOWN messages" do
      ref = make_ref()
      parent = self()

      pool =
        stateless_pool!(
          [
            init: fn next -> {:ok, next} end,
            handle_info: fn msg, next -> send(parent, msg) && {:ok, next} end,
            terminate: fn _, _ -> :ok end
          ],
          pool_size: 2
        )

      send(pool, {:DOWN, ref, :process, parent, :shutdown})

      assert_receive {:DOWN, ^ref, :process, ^parent, :shutdown}
      assert_receive {:DOWN, ^ref, :process, ^parent, :shutdown}
    end

    test "forwards ref messages" do
      ref = make_ref()
      parent = self()

      pool =
        stateless_pool!(
          [
            init: fn next -> {:ok, next} end,
            handle_info: fn msg, next -> send(parent, msg) && {:ok, next} end,
            terminate: fn _, _ -> :ok end
          ],
          pool_size: 2
        )

      send(pool, {ref, :ok})

      assert_receive {^ref, :ok}
      assert_receive {^ref, :ok}
    end

    test "forwards exit messages" do
      parent = self()

      pool =
        stateless_pool!(
          [
            init: fn next -> {:ok, next} end,
            handle_info: fn msg, next -> send(parent, msg) && {:ok, next} end,
            terminate: fn _, _ -> :ok end
          ],
          pool_size: 2
        )

      send(pool, {:EXIT, parent, :normal})

      assert_receive {:EXIT, ^parent, :normal}
      assert_receive {:EXIT, ^parent, :normal}
    end
  end

  describe "async" do
    defp async_init(fun) do
      fn next ->
        {:async,
         fn ->
           fun.()
           next
         end}
      end
    end

    test "lazily initializes a connection" do
      parent = self()

      async_init =
        async_init(fn ->
          send(parent, {:async, self()})
          assert_receive(:release)
        end)

      pool =
        stateless_pool!(
          init: async_init,
          handle_checkout: fn :checkout, _from, next -> {:ok, :client_state_out, next} end,
          handle_checkin: fn :client_state_in, _from, next -> {:ok, next} end,
          terminate: fn reason, [] -> send(parent, {:terminate, reason}) end
        )

      assert_receive {:async, task}

      assert catch_exit(
               NimblePool.checkout!(pool, :checkout, fn _ -> raise "never invoked" end, 50)
             ) ==
               {:timeout, {NimblePool, :checkout, [pool]}}

      send(task, :release)

      assert NimblePool.checkout!(pool, :checkout, fn :client_state_out ->
               {:result, :client_state_in}
             end) ==
               :result
    end

    @tag :capture_log
    test "reinitializes the connection if it fails" do
      parent = self()

      async_init =
        async_init(fn ->
          send(parent, {:async, self()})
          assert_receive({:release, as})
          if as == :error, do: raise("oops"), else: :ok
        end)

      pool =
        stateless_pool!(
          init: async_init,
          handle_checkout: fn :checkout, _from, next -> {:ok, :client_state_out, next} end,
          handle_checkin: fn :client_state_in, _from, next -> {:ok, next} end,
          terminate: fn reason, [] -> send(parent, {:terminate, reason}) end
        )

      assert_receive {:async, task}
      send(task, {:release, :error})

      assert_receive {:async, task}
      send(task, {:release, :ok})

      assert NimblePool.checkout!(pool, :checkout, fn :client_state_out ->
               {:result, :client_state_in}
             end) ==
               :result
    end
  end

  describe "remove" do
    test "removes and restarts on checkout" do
      parent = self()

      {agent, pool} =
        stateful_pool!(
          init: fn next -> send(parent, :started) && {:ok, next} end,
          handle_checkout: fn :checkout, _from, _next -> {:remove, :restarting} end,
          terminate: fn reason, _ -> send(parent, {:terminate, reason}) end,
          init: fn next -> send(parent, :restarted) && {:ok, next} end,
          handle_checkout: fn :checkout, _from, next -> {:ok, :client_state_out, next} end,
          handle_checkin: fn :client_state_in, _from, next -> {:ok, next} end,
          terminate: fn reason, _ -> send(parent, {:terminate, reason}) end
        )

      assert_receive :started

      assert NimblePool.checkout!(pool, :checkout, fn :client_state_out ->
               {:result, :client_state_in}
             end) ==
               :result

      assert_receive :restarted

      NimblePool.stop(pool, :shutdown)
      assert_receive {:terminate, :shutdown}
      assert_drained agent
    end

    test "removes and restarts on checkin" do
      parent = self()

      {agent, pool} =
        stateful_pool!(
          init: fn next -> send(parent, :started) && {:ok, next} end,
          handle_checkout: fn :checkout, _from, next -> {:ok, :client_state_out, next} end,
          handle_checkin: fn :client_state_in, _from, _next -> {:remove, :restarting} end,
          terminate: fn reason, _ -> send(parent, {:terminate, reason}) end,
          init: fn next -> send(parent, :restarted) && {:ok, next} end,
          handle_checkout: fn :checkout2, _from, next -> {:ok, :client_state_out2, next} end,
          handle_checkin: fn :client_state_in2, _from, next -> {:ok, next} end,
          terminate: fn reason, _ -> send(parent, {:terminate, reason}) end
        )

      assert_receive :started

      assert NimblePool.checkout!(pool, :checkout, fn :client_state_out ->
               {:result, :client_state_in}
             end) ==
               :result

      assert_receive :restarted

      assert NimblePool.checkout!(pool, :checkout2, fn :client_state_out2 ->
               {:result, :client_state_in2}
             end) ==
               :result

      NimblePool.stop(pool, :shutdown)
      assert_receive {:terminate, :shutdown}
      assert_drained agent
    end

    test "removes and restarts on handle_info" do
      parent = self()

      {agent, pool} =
        stateful_pool!(
          init: fn next -> send(parent, :started) && {:ok, next} end,
          handle_info: fn :remove, _next -> {:remove, :restarting} end,
          terminate: fn reason, _ -> send(parent, {:terminate, reason}) end,
          init: fn next -> send(parent, :restarted) && {:ok, next} end,
          handle_checkout: fn :checkout, _from, next -> {:ok, :client_state_out, next} end,
          handle_checkin: fn :client_state_in, _from, next -> {:ok, next} end,
          terminate: fn reason, _ -> send(parent, {:terminate, reason}) end
        )

      assert_receive :started
      assert send(pool, :remove)
      assert_receive :restarted

      assert NimblePool.checkout!(pool, :checkout, fn :client_state_out ->
               {:result, :client_state_in}
             end) ==
               :result

      NimblePool.stop(pool, :shutdown)
      assert_receive {:terminate, :shutdown}
      assert_drained agent
    end
  end

  describe "errors" do
    @describetag :capture_log

    test "restarts on init failure without blocking main loop" do
      parent = self()

      {agent, pool} =
        stateful_pool!(
          [
            init: fn next -> {:ok, next} end,
            init: fn next -> {:ok, next} end,
            handle_checkout: fn :checkout, _from, next -> {:ok, :client_state_out, next} end,
            handle_checkin: fn :client_state_in, _from, _next -> {:remove, :checkin} end,
            terminate: fn reason, _ -> send(parent, {:terminate, reason}) end,
            init: fn _next -> assert_receive(:release) && raise "oops" end,
            handle_info: fn message, next -> send(parent, {:info, message}) && {:ok, next} end,
            init: fn _next -> assert_receive(:release) && raise "oops" end,
            init: fn next -> send(parent, :init) && {:ok, next} end,
            terminate: fn reason, _ -> send(parent, {:terminate, reason}) end,
            terminate: fn reason, _ -> send(parent, {:terminate, reason}) end
          ],
          pool_size: 2
        )

      assert NimblePool.checkout!(pool, :checkout, fn :client_state_out ->
               {:result, :client_state_in}
             end) ==
               :result

      assert_receive {:terminate, :checkin}
      send(pool, :not_blocked)
      send(pool, :release)
      assert_receive {:info, :not_blocked}
      send(pool, :release)
      assert_receive :init

      NimblePool.stop(pool, :shutdown)
      assert_receive {:terminate, :shutdown}
      assert_receive {:terminate, :shutdown}
      assert_drained agent
    end

    test "restarts on handle_checkout failure" do
      parent = self()

      {agent, pool} =
        stateful_pool!(
          init: fn next -> {:ok, next} end,
          handle_checkout: fn :checkout, _from, _next -> raise "oops" end,
          terminate: fn reason, _ -> send(parent, {:terminate, reason}) end,
          init: fn next -> {:ok, next} end,
          handle_checkout: fn :checkout, _from, next -> {:ok, :client_state_out, next} end,
          handle_checkin: fn :client_state_in, _from, next -> {:ok, next} end,
          terminate: fn reason, _ -> send(parent, {:terminate, reason}) end
        )

      assert NimblePool.checkout!(pool, :checkout, fn :client_state_out ->
               {:result, :client_state_in}
             end) ==
               :result

      assert_receive {:terminate, %RuntimeError{}}
      NimblePool.stop(pool, :shutdown)
      assert_receive {:terminate, :shutdown}
      assert_drained agent
    end

    test "restarts on handle_checkin failure" do
      parent = self()

      {agent, pool} =
        stateful_pool!(
          init: fn next -> {:ok, next} end,
          handle_checkout: fn :checkout, _from, next -> {:ok, :client_state_out, next} end,
          handle_checkin: fn :client_state_in, _from, _next -> raise "oops" end,
          terminate: fn reason, _ -> send(parent, {:terminate, reason}) end,
          init: fn next -> {:ok, next} end,
          handle_checkout: fn :checkout2, _from, next -> {:ok, :client_state_out2, next} end,
          handle_checkin: fn :client_state_in2, _from, next -> {:ok, next} end,
          terminate: fn reason, _ -> send(parent, {:terminate, reason}) end
        )

      assert NimblePool.checkout!(pool, :checkout, fn :client_state_out ->
               {:result, :client_state_in}
             end) ==
               :result

      assert_receive {:terminate, %RuntimeError{}}

      assert NimblePool.checkout!(pool, :checkout2, fn :client_state_out2 ->
               {:result, :client_state_in2}
             end) ==
               :result

      NimblePool.stop(pool, :shutdown)
      assert_receive {:terminate, :shutdown}
      assert_drained agent
    end

    test "restarts on handle_info failure" do
      parent = self()

      {agent, pool} =
        stateful_pool!(
          init: fn next -> {:ok, next} end,
          handle_info: fn _msg, _next -> raise "oops" end,
          terminate: fn reason, _ -> send(parent, {:terminate, reason}) end,
          init: fn next -> {:ok, next} end,
          handle_checkout: fn :checkout, _from, next -> {:ok, :client_state_out, next} end,
          handle_checkin: fn :client_state_in, _from, next -> {:ok, next} end,
          terminate: fn reason, _ -> send(parent, {:terminate, reason}) end
        )

      send(pool, :oops)
      assert_receive {:terminate, %RuntimeError{}}

      assert NimblePool.checkout!(pool, :checkout, fn :client_state_out ->
               {:result, :client_state_in}
             end) ==
               :result

      NimblePool.stop(pool, :shutdown)
      assert_receive {:terminate, :shutdown}
      assert_drained agent
    end

    test "discards terminate failure" do
      parent = self()

      {agent, pool} =
        stateful_pool!(
          init: fn next -> {:ok, next} end,
          handle_info: fn _msg, _next -> {:remove, :unused} end,
          terminate: fn :unused, _ -> raise "oops" end,
          init: fn next -> send(parent, :init) && {:ok, next} end,
          handle_checkout: fn :checkout, _from, next -> {:ok, :client_state_out, next} end,
          handle_checkin: fn :client_state_in, _from, next -> {:ok, next} end,
          terminate: fn reason, _ -> send(parent, {:terminate, reason}) end
        )

      send(pool, :oops)
      assert_receive :init

      assert NimblePool.checkout!(pool, :checkout, fn :client_state_out ->
               {:result, :client_state_in}
             end) ==
               :result

      NimblePool.stop(pool, :shutdown)
      assert_receive {:terminate, :shutdown}
      assert_drained agent
    end
  end
end
