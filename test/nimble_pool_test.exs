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

    result =
      NimblePool.checkout!(pool, fn :client_state_out ->
        {:result, :client_state_in}
      end)

    assert result == :result

    NimblePool.stop(pool, :shutdown)
    assert_receive {:terminate, :shutdown}
  end
end
