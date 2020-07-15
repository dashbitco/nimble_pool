defmodule NimblePoolTest do
  use ExUnit.Case

  defmodule StatelessPool do
    @behaviour NimblePool

    def init_pool([init_pool: fun] ++ rest), do: fun.(rest)
    def init_pool(rest), do: {:ok, rest}

    def init_worker([init_worker: fun] ++ rest = pool_state) do
      Tuple.append(fun.(rest), pool_state)
    end

    def handle_checkout(command, from, instructions, pool_state) do
      next(instructions, :handle_checkout, &[command, from, &1, pool_state])
    end

    def handle_checkin(client_state, from, instructions, pool_state) do
      next(instructions, :handle_checkin, &[client_state, from, &1, pool_state])
    end

    def handle_info(message, instructions) do
      next(instructions, :handle_info, &[message, &1])
    end

    def terminate_worker(reason, instructions, pool_state) do
      # We always allow skip ahead on terminate
      instructions = Enum.drop_while(instructions, &(elem(&1, 0) != :terminate_worker))
      next(instructions, :terminate_worker, &[reason, &1, pool_state])
    end

    defp next([{instruction, return} | instructions], instruction, args) do
      apply(return, args.(instructions))
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

    def init_worker(pid) do
      TestAgent.next(pid, :init_worker, [pid])
    end

    def handle_checkout(command, from, worker_state, pool_state) do
      TestAgent.next(pool_state, :handle_checkout, [command, from, worker_state, pool_state])
    end

    def handle_checkin(client_state, from, worker_state, pool_state) do
      TestAgent.next(pool_state, :handle_checkin, [client_state, from, worker_state, pool_state])
    end

    def handle_update(command, worker_state, pool_state) do
      TestAgent.next(pool_state, :handle_update, [command, worker_state, pool_state])
    end

    def handle_info(message, worker_state) do
      TestAgent.next(worker_state, :handle_info, [message, worker_state])
    end

    def terminate_worker(reason, worker_state, pid) do
      TestAgent.next(pid, :terminate_worker, [reason, worker_state, pid])
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
        init_pool: fn next ->
          send(parent, :init_pool)
          {:ok, next}
        end,
        init_worker: fn next -> {:ok, next} end,
        handle_checkout: fn :checkout, _from, next, pool_state ->
          {:ok, :client_state_out, next, pool_state}
        end,
        handle_checkin: fn :client_state_in, _from, next, pool_state ->
          {:ok, next, pool_state}
        end,
        terminate_worker: fn reason, [], state ->
          send(parent, {:terminate, reason})
          {:ok, state}
        end
      )

    assert {:messages, [:init_pool | _]} = Process.info(self(), :messages)
    assert_receive :init_pool

    assert NimblePool.checkout!(pool, :checkout, fn _ref, :client_state_out ->
             {:result, :client_state_in}
           end) == :result

    NimblePool.stop(pool, :shutdown)
    assert_receive {:terminate, :shutdown}

    # Assert down from checkout! did not leak
    refute_received {:DOWN, _, _, _, _}
  end

  describe "init" do
    test "raises on invalid return" do
      assert_raise RuntimeError, ~r"{:ok, worker_state, pool_state}", fn ->
        stateless_pool!(init_worker: fn _ -> {:oops} end)
      end
    end
  end

  describe "checkout!" do
    test "exits with noproc for missing process" do
      assert catch_exit(
               NimblePool.checkout!(:unknown, :checkout, fn _ref, state -> {state, state} end)
             ) ==
               {:noproc, {NimblePool, :checkout, [:unknown]}}
    end

    test "exits when pool terminates on checkout" do
      Process.flag(:trap_exit, true)

      pool =
        stateless_pool!(
          init_worker: fn next -> {:ok, next} end,
          handle_checkout: fn :checkout, _from, _next, _pool_state ->
            Process.exit(self(), :kill)
          end
        )

      assert catch_exit(
               NimblePool.checkout!(pool, :checkout, fn _ref, state -> {state, state} end)
             ) ==
               {:killed, {NimblePool, :checkout, [pool]}}
    end

    test "restarts worker on client throw/error/exit during checkout" do
      parent = self()

      pool =
        stateless_pool!(
          init_worker: fn next -> send(parent, :started) && {:ok, next} end,
          handle_checkout: fn :checkout, _from, next, pool_state ->
            {:ok, :client_state_out, next, pool_state}
          end,
          handle_checkin: fn :client_state_in, _from, next, pool_state ->
            {:ok, next, pool_state}
          end,
          terminate_worker: fn reason, [], state ->
            send(parent, {:terminate, reason})
            {:ok, state}
          end
        )

      # Started once
      assert_receive :started
      refute_received :started

      assert_raise RuntimeError, "oops", fn ->
        NimblePool.checkout!(pool, :checkout, fn _ref, :client_state_out -> raise "oops" end)
      end

      # Terminated and restarted
      assert_receive {:terminate, :error}
      assert_receive :started
      refute_received :started

      # Do a proper checkout now
      assert NimblePool.checkout!(pool, :checkout, fn _ref, :client_state_out ->
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
          init_worker: fn next -> send(parent, :started) && {:ok, next} end,
          handle_checkout: fn :checkout, _from, next, pool_state ->
            {:ok, :client_state_out, next, pool_state}
          end,
          handle_checkin: fn :client_state_in, _from, next, pool_state ->
            {:ok, next, pool_state}
          end,
          terminate_worker: fn reason, [], state ->
            send(parent, {:terminate, reason})
            {:ok, state}
          end
        )

      # Started once
      assert_receive :started
      refute_received :started

      {:ok, pid} =
        Task.start(fn ->
          NimblePool.checkout!(pool, :checkout, fn _ref, :client_state_out ->
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
      assert NimblePool.checkout!(pool, :checkout, fn _ref, :client_state_out ->
               {:result, :client_state_in}
             end) == :result

      # Assert down from failed checkout! did not leak
      NimblePool.stop(pool, :shutdown)
      refute_received {:DOWN, _, _, _, _}
      assert_receive {:terminate, :shutdown}
    end

    test "does not restart worker on client timeout during checkout" do
      parent = self()

      pool =
        stateless_pool!(
          init_worker: fn next -> send(parent, :started) && {:ok, next} end,
          handle_checkout: fn :checkout, _from, next, pool_state ->
            {:ok, :client_state_out, next, pool_state}
          end,
          handle_checkin: fn :client_state_in, _from, next, pool_state ->
            {:ok, next, pool_state}
          end,
          terminate_worker: fn reason, [], state ->
            send(parent, {:terminate, reason})
            {:ok, state}
          end
        )

      # Started once
      assert_receive :started
      refute_received :started

      # Suspend the pool, this will trigger a timeout but the message will be delivered
      :sys.suspend(pool)

      assert catch_exit(
               NimblePool.checkout!(pool, :checkout, fn _, _ -> raise "never invoked" end, 0)
             ) ==
               {:timeout, {NimblePool, :checkout, [pool]}}

      :sys.resume(pool)

      # Do a proper checkout now
      assert NimblePool.checkout!(pool, :checkout, fn _ref, :client_state_out ->
               {:result, :client_state_in}
             end) == :result

      # Did not have to start a new worker after the previous timeout
      refute_received :started

      NimblePool.stop(pool, :shutdown)
      assert_receive {:terminate, :shutdown}
    end

    test "does not restart worker on client timeout during unused checkout" do
      parent = self()

      pool =
        stateless_pool!(
          init_worker: fn next -> send(parent, :started) && {:ok, next} end,
          handle_checkout: fn :checkout, _from, next, pool_state ->
            {:ok, :client_state_out, next, pool_state}
          end,
          handle_checkin: fn :client_state_in, _from, next, pool_state ->
            {:ok, next, pool_state}
          end,
          handle_checkout: fn :checkout2, _from, next, pool_state ->
            {:ok, :client_state_out2, next, pool_state}
          end,
          handle_checkin: fn :client_state_in2, _from, next, pool_state ->
            {:ok, next, pool_state}
          end,
          terminate_worker: fn reason, [], state ->
            send(parent, {:terminate, reason})
            {:ok, state}
          end
        )

      # Started once
      assert_receive :started
      refute_received :started

      # Checkout the pool in a separate process
      task =
        Task.async(fn ->
          NimblePool.checkout!(pool, :checkout, fn _ref, :client_state_out ->
            send(parent, :lock)
            assert_receive :release
            {:result, :client_state_in}
          end)
        end)

      assert_receive :lock

      # Now we do a failed checkout
      assert catch_exit(
               NimblePool.checkout!(pool, :checkout, fn _, _ -> raise "never invoked" end, 0)
             ) ==
               {:timeout, {NimblePool, :checkout, [pool]}}

      # And we release the original checkout
      send(task.pid, :release)
      assert Task.await(task) == :result

      # Terminated and restarted
      refute_received {:terminate, :timeout}
      refute_received :started

      # Do a proper checkout now and it still works
      assert NimblePool.checkout!(pool, :checkout2, fn _ref, :client_state_out2 ->
               {:result, :client_state_in2}
             end) == :result
    end

    test "queues checkouts" do
      parent = self()

      pool =
        stateless_pool!(
          init_worker: fn next -> {:ok, next} end,
          handle_checkout: fn :checkout, _from, next, pool_state ->
            {:ok, :client_state_out, next, pool_state}
          end,
          handle_checkin: fn :client_state_in, _from, next, pool_state ->
            {:ok, next, pool_state}
          end,
          handle_checkout: fn :checkout2, _from, next, pool_state ->
            {:ok, :client_state_out2, next, pool_state}
          end,
          handle_checkin: fn :client_state_in2, _from, next, pool_state ->
            {:ok, next, pool_state}
          end,
          terminate_worker: fn _reason, [], state -> {:ok, state} end
        )

      # Checkout the pool in a separate process
      task1 =
        Task.async(fn ->
          NimblePool.checkout!(pool, :checkout, fn _ref, :client_state_out ->
            send(parent, :lock)
            assert_receive :release
            {:result, :client_state_in}
          end)
        end)

      assert_receive :lock

      # Checkout the pool in another process
      task2 =
        Task.async(fn ->
          NimblePool.checkout!(pool, :checkout2, fn _ref, :client_state_out2 ->
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
            init_worker: fn next -> {:ok, next} end,
            handle_checkout: fn :checkout, _from, next, pool_state ->
              {:ok, :client_state_out, next, pool_state}
            end,
            handle_checkin: fn :client_state_in, _from, next, pool_state ->
              {:ok, next, pool_state}
            end,
            terminate_worker: fn _reason, [], state -> {:ok, state} end
          ],
          pool_size: 2
        )

      task1 =
        Task.async(fn ->
          NimblePool.checkout!(pool, :checkout, fn _ref, :client_state_out ->
            send(parent, :lock)
            assert_receive :release
            {:result, :client_state_in}
          end)
        end)

      assert_receive :lock

      task2 =
        Task.async(fn ->
          NimblePool.checkout!(pool, :checkout, fn _ref, :client_state_out ->
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
            init_worker: fn next -> {:ok, next} end,
            handle_info: fn :handle_info, next -> send(parent, :info) && {:ok, next} end,
            terminate_worker: fn _reason, [], state -> {:ok, state} end
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
            init_worker: fn next -> {:ok, next} end,
            handle_info: fn msg, next -> send(parent, msg) && {:ok, next} end,
            terminate_worker: fn _reason, [], state -> {:ok, state} end
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
            init_worker: fn next -> {:ok, next} end,
            handle_info: fn msg, next -> send(parent, msg) && {:ok, next} end,
            terminate_worker: fn _reason, [], state -> {:ok, state} end
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
            init_worker: fn next -> {:ok, next} end,
            handle_info: fn msg, next -> send(parent, msg) && {:ok, next} end,
            terminate_worker: fn _reason, [], state -> {:ok, state} end
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
          init_worker: async_init,
          handle_checkout: fn :checkout, _from, next, pool_state ->
            {:ok, :client_state_out, next, pool_state}
          end,
          handle_checkin: fn :client_state_in, _from, next, pool_state ->
            {:ok, next, pool_state}
          end,
          terminate_worker: fn reason, [], state ->
            send(parent, {:terminate, reason})
            {:ok, state}
          end
        )

      assert_receive {:async, task}

      assert catch_exit(
               NimblePool.checkout!(pool, :checkout, fn _, _ -> raise "never invoked" end, 50)
             ) ==
               {:timeout, {NimblePool, :checkout, [pool]}}

      send(task, :release)

      assert NimblePool.checkout!(pool, :checkout, fn _ref, :client_state_out ->
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
          init_worker: async_init,
          handle_checkout: fn :checkout, _from, next, pool_state ->
            {:ok, :client_state_out, next, pool_state}
          end,
          handle_checkin: fn :client_state_in, _from, next, pool_state ->
            {:ok, next, pool_state}
          end,
          terminate_worker: fn reason, [], state ->
            send(parent, {:terminate, reason})
            {:ok, state}
          end
        )

      assert_receive {:async, task}
      send(task, {:release, :error})

      assert_receive {:async, task}
      send(task, {:release, :ok})

      assert NimblePool.checkout!(pool, :checkout, fn _ref, :client_state_out ->
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
          init_worker: fn next -> send(parent, :started) && {:ok, next, next} end,
          handle_checkout: fn :checkout, _from, _next, pool_state ->
            {:remove, :restarting, pool_state}
          end,
          terminate_worker: fn reason, _, state ->
            send(parent, {:terminate, reason})
            {:ok, state}
          end,
          init_worker: fn next -> send(parent, :restarted) && {:ok, next, next} end,
          handle_checkout: fn :checkout, _from, next, pool_state ->
            {:ok, :client_state_out, next, pool_state}
          end,
          handle_checkin: fn :client_state_in, _from, next, pool_state ->
            {:ok, next, pool_state}
          end,
          terminate_worker: fn reason, _, state ->
            send(parent, {:terminate, reason})
            {:ok, state}
          end
        )

      assert_receive :started

      assert NimblePool.checkout!(pool, :checkout, fn _ref, :client_state_out ->
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
          init_worker: fn next -> send(parent, :started) && {:ok, next, next} end,
          handle_checkout: fn :checkout, _from, next, pool_state ->
            {:ok, :client_state_out, next, pool_state}
          end,
          handle_checkin: fn :client_state_in, _from, _next, pool_state ->
            {:remove, :restarting, pool_state}
          end,
          terminate_worker: fn reason, _, state ->
            send(parent, {:terminate, reason})
            {:ok, state}
          end,
          init_worker: fn next -> send(parent, :restarted) && {:ok, next, next} end,
          handle_checkout: fn :checkout2, _from, next, pool_state ->
            {:ok, :client_state_out2, next, pool_state}
          end,
          handle_checkin: fn :client_state_in2, _from, next, pool_state ->
            {:ok, next, pool_state}
          end,
          terminate_worker: fn reason, _, state ->
            send(parent, {:terminate, reason})
            {:ok, state}
          end
        )

      assert_receive :started

      assert NimblePool.checkout!(pool, :checkout, fn _ref, :client_state_out ->
               {:result, :client_state_in}
             end) ==
               :result

      assert_receive :restarted

      assert NimblePool.checkout!(pool, :checkout2, fn _ref, :client_state_out2 ->
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
          init_worker: fn next -> send(parent, :started) && {:ok, next, next} end,
          handle_info: fn :remove, _next -> {:remove, :restarting} end,
          terminate_worker: fn reason, _, state ->
            send(parent, {:terminate, reason})
            {:ok, state}
          end,
          init_worker: fn next -> send(parent, :restarted) && {:ok, next, next} end,
          handle_checkout: fn :checkout, _from, next, pool_state ->
            {:ok, :client_state_out, next, pool_state}
          end,
          handle_checkin: fn :client_state_in, _from, next, pool_state ->
            {:ok, next, pool_state}
          end,
          terminate_worker: fn reason, _, state ->
            send(parent, {:terminate, reason})
            {:ok, state}
          end
        )

      assert_receive :started
      assert send(pool, :remove)
      assert_receive :restarted

      assert NimblePool.checkout!(pool, :checkout, fn _ref, :client_state_out ->
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

    test "restarts on client exit/throw/error during checkout" do
      parent = self()

      {agent, pool} =
        stateful_pool!(
          init_worker: fn next -> {:ok, next, next} end,
          handle_checkout: fn :checkout, _from, _next, pool_state ->
            {:ok, :client_state_out, :server_state_out, pool_state}
          end,
          terminate_worker: fn reason, :server_state_out, state ->
            send(parent, {:terminate, reason})
            {:ok, state}
          end,
          init_worker: fn next -> {:ok, next, next} end,
          terminate_worker: fn reason, _, state ->
            send(parent, {:terminate, reason})
            {:ok, state}
          end
        )

      assert_raise RuntimeError, fn ->
        NimblePool.checkout!(pool, :checkout, fn _ref, :client_state_out ->
          raise "oops"
        end)
      end

      assert_receive {:terminate, :error}
      NimblePool.stop(pool, :shutdown)
      assert_receive {:terminate, :shutdown}
      assert_drained agent
    end

    test "restarts on client exit/throw/error during checkout with updated state" do
      parent = self()

      {agent, pool} =
        stateful_pool!(
          init_worker: fn next -> {:ok, next, next} end,
          handle_checkout: fn :checkout, _from, _next, pool_state ->
            {:ok, :client_state_out, :server_state_out, pool_state}
          end,
          handle_update: fn :update, _next, pool_state ->
            {:ok, :updated_state, pool_state}
          end,
          terminate_worker: fn reason, :updated_state, pool_state ->
            send(parent, {:terminate, reason})
            {:ok, pool_state}
          end,
          init_worker: fn next -> {:ok, next, next} end,
          terminate_worker: fn reason, _, pool_state ->
            send(parent, {:terminate, reason})
            {:ok, pool_state}
          end
        )

      assert_raise RuntimeError, fn ->
        NimblePool.checkout!(pool, :checkout, fn ref, :client_state_out ->
          NimblePool.update(ref, :update)
          raise "oops"
        end)
      end

      assert_receive {:terminate, :error}
      NimblePool.stop(pool, :shutdown)
      assert_receive {:terminate, :shutdown}
      assert_drained agent
    end

    test "restarts on init failure without blocking main loop" do
      parent = self()

      {agent, pool} =
        stateful_pool!(
          [
            init_worker: fn next -> {:ok, next, next} end,
            init_worker: fn next -> {:ok, next, next} end,
            handle_checkout: fn :checkout, _from, next, pool_state ->
              {:ok, :client_state_out, next, pool_state}
            end,
            handle_checkin: fn :client_state_in, _from, _next, pool_state ->
              {:remove, :checkin, pool_state}
            end,
            terminate_worker: fn reason, _, state ->
              send(parent, {:terminate, reason})
              {:ok, state}
            end,
            init_worker: fn _next -> assert_receive(:release) && raise "oops" end,
            handle_info: fn message, next -> send(parent, {:info, message}) && {:ok, next} end,
            init_worker: fn _next -> assert_receive(:release) && raise "oops" end,
            init_worker: fn next -> send(parent, :init) && {:ok, next, next} end,
            terminate_worker: fn reason, _, state ->
              send(parent, {:terminate, reason})
              {:ok, state}
            end,
            terminate_worker: fn reason, _, state ->
              send(parent, {:terminate, reason})
              {:ok, state}
            end
          ],
          pool_size: 2
        )

      assert NimblePool.checkout!(pool, :checkout, fn _ref, :client_state_out ->
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
          init_worker: fn next -> {:ok, next, next} end,
          handle_checkout: fn :checkout, _from, _next, _pool_state -> raise "oops" end,
          terminate_worker: fn reason, _, state ->
            send(parent, {:terminate, reason})
            {:ok, state}
          end,
          init_worker: fn next -> {:ok, next, next} end,
          handle_checkout: fn :checkout, _from, next, pool_state ->
            {:ok, :client_state_out, next, pool_state}
          end,
          handle_checkin: fn :client_state_in, _from, next, pool_state ->
            {:ok, next, pool_state}
          end,
          terminate_worker: fn reason, _, state ->
            send(parent, {:terminate, reason})
            {:ok, state}
          end
        )

      assert NimblePool.checkout!(pool, :checkout, fn _ref, :client_state_out ->
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
          init_worker: fn next -> {:ok, next, next} end,
          handle_checkout: fn :checkout, _from, next, pool_state ->
            {:ok, :client_state_out, next, pool_state}
          end,
          handle_checkin: fn :client_state_in, _from, _next, _pool_state -> raise "oops" end,
          terminate_worker: fn reason, _, state ->
            send(parent, {:terminate, reason})
            {:ok, state}
          end,
          init_worker: fn next -> {:ok, next, next} end,
          handle_checkout: fn :checkout2, _from, next, pool_state ->
            {:ok, :client_state_out2, next, pool_state}
          end,
          handle_checkin: fn :client_state_in2, _from, next, pool_state ->
            {:ok, next, pool_state}
          end,
          terminate_worker: fn reason, _, state ->
            send(parent, {:terminate, reason})
            {:ok, state}
          end
        )

      assert NimblePool.checkout!(pool, :checkout, fn _ref, :client_state_out ->
               {:result, :client_state_in}
             end) ==
               :result

      assert_receive {:terminate, %RuntimeError{}}

      assert NimblePool.checkout!(pool, :checkout2, fn _ref, :client_state_out2 ->
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
          init_worker: fn next -> {:ok, next, next} end,
          handle_info: fn _msg, _next -> raise "oops" end,
          terminate_worker: fn reason, _, state ->
            send(parent, {:terminate, reason})
            {:ok, state}
          end,
          init_worker: fn next -> {:ok, next, next} end,
          handle_checkout: fn :checkout, _from, next, pool_state ->
            {:ok, :client_state_out, next, pool_state}
          end,
          handle_checkin: fn :client_state_in, _from, next, pool_state ->
            {:ok, next, pool_state}
          end,
          terminate_worker: fn reason, _, state ->
            send(parent, {:terminate, reason})
            {:ok, state}
          end
        )

      send(pool, :oops)
      assert_receive {:terminate, %RuntimeError{}}

      assert NimblePool.checkout!(pool, :checkout, fn _ref, :client_state_out ->
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
          init_worker: fn next -> {:ok, next, next} end,
          handle_info: fn _msg, _next -> {:remove, :unused} end,
          terminate_worker: fn :unused, _, _ -> raise "oops" end,
          init_worker: fn next -> send(parent, :init) && {:ok, next, next} end,
          handle_checkout: fn :checkout, _from, next, pool_state ->
            {:ok, :client_state_out, next, pool_state}
          end,
          handle_checkin: fn :client_state_in, _from, next, pool_state ->
            {:ok, next, pool_state}
          end,
          terminate_worker: fn reason, _, state ->
            send(parent, {:terminate, reason})
            {:ok, state}
          end
        )

      send(pool, :oops)
      assert_receive :init

      assert NimblePool.checkout!(pool, :checkout, fn _ref, :client_state_out ->
               {:result, :client_state_in}
             end) ==
               :result

      NimblePool.stop(pool, :shutdown)
      assert_receive {:terminate, :shutdown}
      assert_drained agent
    end
  end

  describe "handle_enqueue" do
    test "executes handle_enqueue/2 when defined" do
      defmodule PoolWithHandleEnqueue do
        @behaviour NimblePool

        def init_worker(:parent) do
          {:ok, :child1, :parent1}
        end

        def handle_enqueue(:command, :parent1) do
          {:ok, :wrapped_command, :parent2}
        end

        def handle_checkout(:wrapped_command, _from, :child1, :parent2) do
          {:ok, :client_command, :child2, :parent3}
        end

        def handle_checkin(:checkin_command, _from, :child2, :parent3) do
          {:ok, :child3, :parent4}
        end
      end

      pool = start_pool!(PoolWithHandleEnqueue, :parent, [])

      assert NimblePool.checkout!(pool, :command, fn _, :client_command ->
               {:ok, :checkin_command}
             end) == :ok

      NimblePool.stop(pool, :shutdown)
    end
  end

  describe "skip" do
    defmodule SkippedCheckoutException do
      defexception message: "skipped checkout!"
    end

    test "skips checkout and does not restart worker when handle_checkout returns :skip tuple" do
      defmodule PoolThatSkipsOnHandleCheckout do
        @behaviour NimblePool

        def init_worker(parent) do
          send(parent, :init_worker)
          {:ok, parent, parent}
        end

        def handle_checkout(:skip, _from, _parent, pool_state) do
          {:skip, SkippedCheckoutException, pool_state}
        end

        def handle_checkout(_command, _from, parent, pool_state) do
          {:ok, parent, parent, pool_state}
        end
      end

      parent = self()
      pool = start_pool!(PoolThatSkipsOnHandleCheckout, parent, [])

      assert_receive :init_worker

      assert_raise(
        SkippedCheckoutException,
        ~r/skipped checkout!/,
        fn ->
          NimblePool.checkout!(pool, :skip, fn _ref, state -> {state, state} end)
        end
      )

      refute_receive :init_worker

      assert NimblePool.checkout!(pool, :checkout, fn _ref, state -> {:result, state} end) ==
               :result
    end

    test "handle_checkout will not be called and worker will not restart if handle_enqueue returns :skip tuple" do
      defmodule PoolThatSkipsOnEnqueue do
        @behaviour NimblePool

        def init_worker(:init) do
          {:ok, :client, :will_skip}
        end

        def handle_checkout(:command, _from, :client, :handle_checkout) do
          {:ok, :client_command, :client, :checked_out}
        end

        def handle_enqueue(:skip, :will_skip) do
          {:skip, SkippedCheckoutException, :will_checkout}
        end

        def handle_enqueue(:checkout, :will_checkout) do
          {:ok, :command, :handle_checkout}
        end
      end

      pool = start_pool!(PoolThatSkipsOnEnqueue, :init, [])

      assert_raise(SkippedCheckoutException, ~r/skipped checkout/, fn ->
        NimblePool.checkout!(pool, :skip, fn _ref, :client_command ->
          raise "this will never be called"
        end)
      end)

      assert NimblePool.checkout!(pool, :checkout, fn _ref, :client_command -> {:ok, :ok} end) ==
               :ok
    end
  end
end
