defmodule NimblePool do
  use GenServer
  require Logger

  @type from :: {pid, reference}
  @type server_state :: term
  @type client_state :: term
  @type user_reason :: term

  @callback init(term) ::
              {:ok, server_state}

  @callback handle_checkout(from, server_state) ::
              {:ok, client_state, server_state} | {:remove, user_reason}

  @callback handle_checkin(client_state, from, server_state) ::
              {:ok, server_state} | {:remove, user_reason}

  @callback handle_info(from, server_state) ::
              {:ok, server_state} | {:remove, user_reason}

  @callback terminate(:timeout | :down | :throw | :error | :exit | user_reason, server_state) ::
              :ok

  @optional_callbacks handle_info: 2, terminate: 2

  def child_spec(opts) do
    {worker, _} = Keyword.fetch!(opts, :worker)
    {restart, opts} = Keyword.pop(opts, :restart, :permanent)
    {shutdown, opts} = Keyword.pop(opts, :shutdown, 5_000)

    %{
      id: worker,
      start: {__MODULE__, :start_link, [opts]},
      shutdown: shutdown,
      restart: restart
    }
  end

  def start_link(opts) do
    {{worker, arg}, opts} = Keyword.pop(opts, :worker)
    {pool_size, opts} = Keyword.pop(opts, :pool_size)

    unless is_atom(worker) do
      raise ArgumentError, "worker must be an atom, got: #{inspect(worker)}"
    end

    unless pool_size > 0 do
      raise ArgumentError, "pool_size must be more than 0, got: #{inspect(pool_size)}"
    end

    GenServer.start_link(__MODULE__, {worker, arg, pool_size}, opts)
  end

  def checkout!(pool, fun, timeout \\ 5_000) do
    # Reimplementation of gen.erl call to avoid multiple monitors.
    pid = GenServer.whereis(pool)

    unless pid do
      exit(:noproc)
    end

    ref = Process.monitor(pid)
    send_call(pid, ref, :checkout)

    receive do
      {^ref, worker_client_state} ->
        try do
          fun.(worker_client_state)
        catch
          kind, reason ->
            send_remove(pid, ref, kind)
            :erlang.raise(kind, reason, __STACKTRACE__)
        else
          {result, worker_client_state} ->
            checkin!(pid, ref, worker_client_state, timeout)
            result
        end

      {:DOWN, ^ref, _, _, :noconnection} ->
        exit!({:nodedown, node(pid)}, :checkout, [pid])

      {:DOWN, ^ref, _, _, reason} ->
        exit!(reason, :checkout, [pid])
    after
      timeout ->
        send_remove(pid, ref, :timeout)
        exit!(:timeout, :checkout, [pid])
    end
  end

  defp send_call(pid, ref, message) do
    # Auto-connect is asynchronous. But we still use :noconnect to make sure
    # we send on the monitored connection, and not trigger a new auto-connect.
    Process.send(pid, {:"$gen_call", {self(), ref}, message}, [:noconnect])
  end

  defp send_remove(pid, ref, reason) do
    send(pid, {__MODULE__, ref, reason})
    Process.demonitor(ref, [:flush])
  end

  defp checkin!(pid, ref, worker_client_state, timeout) do
    send_call(pid, ref, {:checkin, worker_client_state})

    receive do
      {^ref, :ok} -> :ok
      {:DOWN, ^ref, _, _, :noconnection} -> exit!({:nodedown, node(pid)}, :checkin, [pid])
      {:DOWN, ^ref, _, _, reason} -> exit!(reason, :checkin, [pid])
    after
      timeout ->
        Process.demonitor(ref, [:flush])
        exit!(:timeout, :checkin, [pid])
    end
  end

  defp exit!(reason, fun, args) do
    exit({reason, {__MODULE__, fun, args}})
  end

  ## Callbacks

  @impl true
  def init({worker, arg, pool_size}) do
    resources =
      Enum.reduce(1..pool_size, :queue.new(), fn _, acc ->
        :queue.in(init_worker(worker, arg), acc)
      end)

    state = %{
      resources: resources,
      worker: worker,
      arg: arg,
      queue: :queue.new(),
      requests: %{},
      monitors: %{}
    }

    {:ok, state}
  end

  @impl true
  def handle_call(:checkout, {pid, ref} = from, state) do
    %{requests: requests, monitors: monitors} = state
    mon_ref = Process.monitor(pid)
    requests = Map.put(requests, ref, {pid, mon_ref})
    monitors = Map.put(monitors, mon_ref, ref)
    state = %{state | requests: requests, monitors: monitors}
    {:noreply, maybe_checkout(mon_ref, from, state)}
  end

  @impl true
  def handle_call({:checkin, worker_client_state}, {pid, ref} = from, state) do
    %{requests: requests, resources: resources, worker: worker, monitors: monitors} = state

    case requests do
      %{^ref => {^pid, mon_ref, worker_server_state}} ->
        checkin =
          if function_exported?(worker, :handle_checkin, 3) do
            worker.handle_checkin(worker_client_state, from, worker_server_state)
          else
            {:ok, worker_server_state}
          end

        GenServer.reply(from, :ok)

        resources =
          case checkin do
            {:ok, worker_server_state} ->
              :queue.in(worker_server_state, resources)

            {:remove, reason} ->
              maybe_terminate(reason, worker_server_state, resources, state)
          end

        Process.demonitor(mon_ref, [:flush])
        monitors = Map.delete(monitors, mon_ref)
        requests = Map.delete(requests, ref)
        state = %{state | requests: requests, monitors: monitors, resources: resources}
        {:noreply, maybe_checkout(state)}

      %{} ->
        exit(:unexpected_checkin)
    end
  end

  @impl true
  def handle_info({__MODULE__, ref, reason}, state) do
    remove_by_ref(ref, reason, state)
  end

  @impl true
  def handle_info({:DOWN, mon_ref, _, _, _} = down, %{monitors: monitors} = state) do
    case monitors do
      %{^mon_ref => ref} -> remove_by_ref(ref, :DOWN, state)
      %{} -> maybe_handle_info(down, state)
    end
  end

  @impl true
  def handle_info(msg, state) do
    maybe_handle_info(msg, state)
  end

  defp remove_by_ref(ref, reason, state) do
    %{resources: resources, requests: requests, monitors: monitors} = state

    case requests do
      # Exited or timed out before we could serve it
      %{^ref => {_, mon_ref}} ->
        Process.demonitor(mon_ref, [:flush])
        monitors = Map.delete(monitors, mon_ref)
        requests = Map.delete(requests, ref)
        {:noreply, %{state | requests: requests, monitors: monitors}}

      # Exited or errored during client processing
      %{^ref => {_, mon_ref, worker_server_state}} ->
        Process.demonitor(mon_ref, [:flush])
        monitors = Map.delete(monitors, mon_ref)
        requests = Map.delete(requests, ref)
        resources = maybe_terminate(reason, worker_server_state, resources, state)
        {:noreply, %{state | requests: requests, monitors: monitors, resources: resources}}

      %{} ->
        exit(:unexpected_remove)
    end
  end

  defp maybe_handle_info(msg, %{resources: resources, worker: worker} = state) do
    if function_exported?(worker, :handle_info, 2) do
      resources =
        Enum.reduce(:queue.to_list(resources), :queue.new(), fn worker_server_state, acc ->
          case worker.handle_info(msg, worker_server_state) do
            {:ok, worker_server_state} -> :queue.in(worker_server_state, acc)
            {:remove, reason} -> maybe_terminate(reason, worker_server_state, acc, state)
          end
        end)

      {:noreply, %{state | resources: resources}}
    else
      {:noreply, state}
    end
  end

  defp maybe_checkout(%{queue: queue, requests: requests} = state) do
    case :queue.out(queue) do
      {{:value, {pid, ref}}, queue} ->
        case requests do
          # The request still exists, so we are good to go
          %{^ref => {^pid, mon_ref}} ->
            maybe_checkout(mon_ref, {pid, ref}, %{state | queue: queue})

          # It should never happen
          %{^ref => _} ->
            exit(:unexpected_checkout)

          # The request is no longer active, do nothing
          %{} ->
            %{state | queue: queue}
        end

      {:empty, _queue} ->
        state
    end
  end

  defp maybe_checkout(mon_ref, {pid, ref} = from, state) do
    %{resources: resources, requests: requests, worker: worker, queue: queue} = state

    case :queue.out(resources) do
      {{:value, worker_server_state}, resources} ->
        case worker.handle_checkout(from, worker_server_state) do
          {:ok, worker_client_state, worker_server_state} ->
            GenServer.reply({pid, ref}, worker_client_state)
            requests = Map.put(requests, ref, {pid, mon_ref, worker_server_state})
            %{state | resources: resources, requests: requests}

          {:remove, reason} ->
            resources = maybe_terminate(reason, worker_server_state, resources, state)
            maybe_checkout(mon_ref, from, %{state | resources: resources})
        end

      {:empty, _} ->
        %{state | queue: :queue.in(from, queue)}
    end
  end

  defp maybe_terminate(reason, worker_server_state, resources, %{worker: worker, arg: arg}) do
    if function_exported?(worker, :terminate, 2) do
      worker.terminate(reason, worker_server_state)
    end

    :queue.in(init_worker(worker, arg), resources)
  end

  defp init_worker(worker, arg) do
    case worker.init(arg) do
      {:ok, worker_state} ->
        worker_state

      other ->
        raise """
        unexpected return from #{inspect(worker)}.init/1.

        Expected: {:ok, state}
        Got: #{inspect(other)}
        """
    end
  end
end
