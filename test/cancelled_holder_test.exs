defmodule NimblePool.CancelledHolderTest do
  use ExUnit.Case, async: true

  defmodule Worker do
    @behaviour NimblePool

    def init_worker(parent), do: {:ok, :resource, parent}

    def handle_enqueue(command, parent) do
      send(parent, {:enqueued, command})
      {:ok, command, parent}
    end

    def handle_checkout(_command, _from, resource, parent) do
      {:ok, resource, resource, parent}
    end
  end

  for lazy <- [true, false], cancellation <- [:down, :exit] do
    test "queued caller progresses after #{cancellation} with lazy=#{lazy}" do
      parent = self()

      pool =
        start_supervised!(
          {NimblePool, worker: {Worker, parent}, pool_size: 1, lazy: unquote(lazy)}
        )

      holder =
        spawn(fn ->
          catch_exit(
            NimblePool.checkout!(pool, :hold, fn _, _ ->
              send(parent, :held)
              receive do: (:cancel -> exit(:cancelled))
            end)
          )
        end)

      on_exit(fn -> Process.exit(holder, :kill) end)
      assert_receive :held

      waiter =
        spawn(fn ->
          NimblePool.checkout!(pool, :wait, fn _, resource ->
            send(parent, :admitted)
            {:ok, resource}
          end)
        end)

      on_exit(fn -> Process.exit(waiter, :kill) end)
      assert_receive {:enqueued, :wait}

      case unquote(cancellation) do
        :down -> Process.exit(holder, :kill)
        :exit -> send(holder, :cancel)
      end

      assert_receive :admitted, 1_000
    end
  end
end
