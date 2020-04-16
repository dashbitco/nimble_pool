defmodule NimblePool do
  @moduledoc "README.md"
             |> File.read!()
             |> String.split("<!-- MDOC !-->")
             |> Enum.fetch!(1)

  use GenServer
  require Logger

  @type from :: {pid, reference}
  @type worker_state :: term
  @type client_state :: term
  @type user_reason :: term

  @doc """
  Initializes the worker.

  It must return `{:ok, worker_state}` or `{:async, fun}`, where the
  `fun` is a zero-arity function that must return the worker state.

  Note this callback is synchronous and therefore will block the pool.
  If you need to perform long initialization, consider using the
  `{:async, fun}` return type.
  """
  @callback init(term) ::
              {:ok, worker_state} | {:async, (() -> worker_state)}

  @doc """
  Initializes the pool.

  Receives the argument passed to `start_link/1` and must return `:ok` upon
  successful initialization, `:ignore` to exit normally, or `{:stop, reason}`
  to exit with `reason` and return `{:error, reason}`.

  This is a good place to perform a registration for example.

  This callback is optional.
  """
  @callback init_pool(term) :: :ok | :ignore | {:stop, reason :: any()}

  @doc """
  Checks a worker out.

  It receives the `command`, given to on `checkout!/4` and it must
  return either `{:ok, client_state, worker_state}` or `{:remove, reason}`.
  If `:remove` is returned, `NimblePool` will attempt to checkout another
  worker.

  Note this callback is synchronous and therefore will block the pool.
  Avoid performing long work in here, instead do as much work as
  possible on the client.
  """
  @callback handle_checkout(command :: term, from, worker_state) ::
              {:ok, client_state, worker_state} | {:remove, user_reason}

  @doc """
  Checks a worker in.

  It receives the `client_state`, returned by the `checkout!/4` anonymous
  function and it must return either `{:ok, worker_state}` or `{:remove, reason}`.

  Note this callback is synchronous and therefore will block the pool.
  Avoid performing long work in here, instead do as much work as
  possible on the client.

  This callback is optional.
  """
  @callback handle_checkin(client_state, from, worker_state) ::
              {:ok, worker_state} | {:remove, user_reason}

  @doc """
  Receives a message in the worker.

  It receives the `message` and it must return either
  `{:ok, worker_state}` or `{:remove, reason}`.

  Note this callback is synchronous and therefore will block the pool.
  Avoid performing long work in here.

  This callback is optional.
  """
  @callback handle_info(message :: term, worker_state) ::
              {:ok, worker_state} | {:remove, user_reason}

  @doc """
  Terminates a worker.

  This callback is invoked with `:DOWN` whenever the client
  link breaks, with `:timeout` whenever the client times out,
  with one of `:throw`, `:error`, `:exit` whenever the client
  crashes with one of the reasons above.

  If at any point you return `{:remove, reason}`, the `reason`
  will also be given to `terminate`. If any callback raises,
  the raised exception will be given as `reason`.

  It receives the latest known `worker_state`, which may not
  be the latest state. For example, if a client checksout the
  state and crashes, we don't fully know the `client_state`,
  so the `terminate` callback needs to take such scenarios
  into account.

  This callback is optional.
  """
  @callback terminate(:DOWN | :timeout | :throw | :error | :exit | user_reason, worker_state) ::
              :ok

  @optional_callbacks init_pool: 1, handle_checkin: 3, handle_info: 2, terminate: 2

  @doc """
  Defines a pool to be started under the supervision tree.

  It accepts the same options as `start_link/1` with the
  addition or `:restart` and `:shutdown` that control the
  "Child Specification".
  """
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

  @doc """
  Starts a pool.

  ## Options

    * `:worker` - a `{worker_mod, worker_init_arg}` tuple with the worker
      module that implements the `NimblePool` behaviour and the worker
      initial argument. This argument is required.

    * `:pool_size - how many workers in the pool. Defaults to 10.

    * `:min_ready` - how many workers must have initialized before the pool
    completes initialization, even if they have an async init callback? Defaults to 0.

  """
  def start_link(opts) do
    {{worker, arg}, opts} = Keyword.pop(opts, :worker)
    {pool_size, opts} = Keyword.pop(opts, :pool_size, 10)
    {min_ready, opts} = Keyword.pop(opts, :min_ready, 0)

    unless is_atom(worker) do
      raise ArgumentError, "worker must be an atom, got: #{inspect(worker)}"
    end

    unless pool_size > 0 do
      raise ArgumentError, "pool_size must be more than 0, got: #{inspect(pool_size)}"
    end

    unless min_ready <= pool_size do
      raise ArgumentError,
            "min_ready must be less than or equal to pool_size, got: #{inspect(pool_size)}"
    end

    worker_opts = %{size: pool_size, min_ready: min_ready}
    GenServer.start_link(__MODULE__, {worker, arg, worker_opts}, opts)
  end

  @doc """
  Stops a pool.
  """
  def stop(pool, reason \\ :normal, timeout \\ :infinity) do
    GenServer.stop(pool, reason, timeout)
  end

  @doc """
  Checks out from the pool.

  It expects a command, which will be passed to the `c:handle_checkout/3`
  callback. The `c:handle_checkout/3` callback will return a client state,
  which is given to the `function`. The `function` must return a two-element
  tuple, where the first element is the function return value, and the second
  element is the updated `client_state`, which will be given as the first
  argument to `c:handle_checkin/3`.

  `checkout!` also has an optional `timeout` value, this value will be applied
  to checkout and checkin operations independently.
  """
  def checkout!(pool, command, function, timeout \\ 5_000) when is_function(function, 1) do
    # Reimplementation of gen.erl call to avoid multiple monitors.
    pid = GenServer.whereis(pool)

    unless pid do
      exit!(:noproc, :checkout, [pool])
    end

    ref = Process.monitor(pid)
    send_call(pid, ref, {:checkout, command, deadline(timeout)})

    receive do
      {^ref, worker_client_state} ->
        try do
          function.(worker_client_state)
        catch
          kind, reason ->
            send(pid, {__MODULE__, ref, kind})
            Process.demonitor(ref, [:flush])
            :erlang.raise(kind, reason, __STACKTRACE__)
        else
          {result, worker_client_state} ->
            checkin!(pid, ref, worker_client_state, timeout)
            result
        end

      {:DOWN, ^ref, _, _, :noconnection} ->
        exit!({:nodedown, get_node(pid)}, :checkout, [pool])

      {:DOWN, ^ref, _, _, reason} ->
        exit!(reason, :checkout, [pool])
    after
      timeout ->
        Process.demonitor(ref, [:flush])
        exit!(:timeout, :checkout, [pool])
    end
  end

  defp deadline(timeout) when is_integer(timeout) do
    System.monotonic_time() + System.convert_time_unit(timeout, :millisecond, :native)
  end

  defp deadline(:infinity), do: :infinity

  defp get_node({_, node}), do: node
  defp get_node(pid) when is_pid(pid), do: node(pid)

  defp send_call(pid, ref, message) do
    # Auto-connect is asynchronous. But we still use :noconnect to make sure
    # we send on the monitored connection, and not trigger a new auto-connect.
    Process.send(pid, {:"$gen_call", {self(), ref}, message}, [:noconnect])
  end

  defp checkin!(pid, ref, worker_client_state, timeout) do
    send_call(pid, ref, {:checkin, worker_client_state})

    receive do
      {^ref, :ok} ->
        Process.demonitor(ref, [:flush])
        :ok

      {:DOWN, ^ref, _, _, :noconnection} ->
        exit!({:nodedown, node(pid)}, :checkin, [pid])

      {:DOWN, ^ref, _, _, reason} ->
        exit!(reason, :checkin, [pid])
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
  def init({worker, arg, pool_opts} = init_args) do
    Process.flag(:trap_exit, true)

    with :ok <- do_init_pool(init_args) do
      {resources, async} =
        Enum.reduce(1..pool_opts.size, {:queue.new(), %{}}, fn i, {resources, async} ->
          if pool_opts.min_ready >= i do
            force_init_worker(worker, arg, resources, async)
          else
            init_worker(worker, arg, resources, async)
          end
        end)

      state = %{
        worker: worker,
        arg: arg,
        queue: :queue.new(),
        requests: %{},
        monitors: %{},
        resources: resources,
        async: async
      }

      {:ok, state}
    end
  end

  @impl true
  def handle_call({:checkout, command, deadline}, {pid, ref} = from, state) do
    %{requests: requests, monitors: monitors} = state
    mon_ref = Process.monitor(pid)
    requests = Map.put(requests, ref, {pid, mon_ref, :command, command, deadline})
    monitors = Map.put(monitors, mon_ref, ref)
    state = %{state | requests: requests, monitors: monitors}
    {:noreply, maybe_checkout(command, mon_ref, deadline, from, state)}
  end

  @impl true
  def handle_call({:checkin, worker_client_state}, {pid, ref} = from, state) do
    %{requests: requests, resources: resources, worker: worker, monitors: monitors} = state

    case requests do
      %{^ref => {^pid, mon_ref, :state, worker_server_state}} ->
        checkin =
          if function_exported?(worker, :handle_checkin, 3) do
            args = [worker_client_state, from, worker_server_state]
            apply_callback(worker, :handle_checkin, args)
          else
            {:ok, worker_server_state}
          end

        GenServer.reply(from, :ok)

        resources =
          case checkin do
            {:ok, worker_server_state} ->
              :queue.in(worker_server_state, resources)

            {:remove, reason} ->
              :ok = remove(reason, worker_server_state, state)
              resources
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
    cancel_request_ref(ref, reason, state)
  end

  @impl true
  def handle_info({__MODULE__, :init}, state) do
    %{async: async, resources: resources, worker: worker, arg: arg} = state
    {resources, async} = init_worker(worker, arg, resources, async)
    {:noreply, maybe_checkout(%{state | async: async, resources: resources})}
  end

  @impl true
  def handle_info({:DOWN, ref, _, _, _} = down, state) do
    %{monitors: monitors, async: async} = state

    case monitors do
      %{^ref => request_ref} ->
        cancel_request_ref(request_ref, :DOWN, state)

      %{} ->
        case async do
          %{^ref => _} -> remove_async_ref(ref, state)
          %{} -> maybe_handle_info(down, state)
        end
    end
  end

  @impl true
  def handle_info({ref, worker_state} = reply, state) when is_reference(ref) do
    %{async: async, resources: resources} = state

    case async do
      %{^ref => _} ->
        Process.demonitor(ref, [:flush])
        resources = :queue.in(worker_state, resources)
        async = Map.delete(async, ref)
        state = %{state | async: async, resources: resources}
        {:noreply, maybe_checkout(state)}

      %{} ->
        maybe_handle_info(reply, state)
    end
  end

  @impl true
  def handle_info({:EXIT, pid, _reason} = exit, state) do
    %{async: async} = state

    case async do
      %{^pid => _} -> {:noreply, %{state | async: Map.delete(async, pid)}}
      %{} -> maybe_handle_info(exit, state)
    end
  end

  @impl true
  def handle_info(msg, state) do
    maybe_handle_info(msg, state)
  end

  @impl true
  def terminate(reason, %{worker: worker, resources: resources}) do
    for worker_server_state <- :queue.to_list(resources) do
      maybe_terminate(worker, reason, worker_server_state)
    end

    :ok
  end

  defp do_init_pool({worker, _arg, _pool_size} = init_args) do
    if function_exported?(worker, :init_pool, 1) do
      worker.init_pool(init_args)
    else
      :ok
    end
  end

  defp remove_async_ref(ref, state) do
    %{async: async, resources: resources, worker: worker, arg: arg} = state
    {resources, async} = init_worker(worker, arg, resources, Map.delete(async, ref))
    {:noreply, %{state | resources: resources, async: async}}
  end

  defp cancel_request_ref(ref, reason, state) do
    %{resources: resources, requests: requests, monitors: monitors} = state

    case requests do
      # Exited or timed out before we could serve it
      %{^ref => {_, mon_ref, :command, _, _}} ->
        Process.demonitor(mon_ref, [:flush])
        monitors = Map.delete(monitors, mon_ref)
        requests = Map.delete(requests, ref)
        {:noreply, %{state | requests: requests, monitors: monitors}}

      # Exited or errored during client processing
      %{^ref => {_, mon_ref, :state, worker_server_state}} ->
        Process.demonitor(mon_ref, [:flush])
        monitors = Map.delete(monitors, mon_ref)
        requests = Map.delete(requests, ref)
        :ok = remove(reason, worker_server_state, state)
        state = %{state | requests: requests, monitors: monitors, resources: resources}
        {:noreply, state}

      %{} ->
        exit(:unexpected_remove)
    end
  end

  defp maybe_handle_info(msg, state) do
    %{resources: resources, worker: worker} = state

    if function_exported?(worker, :handle_info, 2) do
      resources =
        Enum.reduce(:queue.to_list(resources), :queue.new(), fn
          worker_server_state, resources ->
            case apply_callback(worker, :handle_info, [msg, worker_server_state]) do
              {:ok, worker_server_state} ->
                :queue.in(worker_server_state, resources)

              {:remove, reason} ->
                :ok = remove(reason, worker_server_state, state)
                resources
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
          %{^ref => {^pid, mon_ref, :command, command, deadline}} ->
            maybe_checkout(command, mon_ref, deadline, {pid, ref}, %{state | queue: queue})

          # It should never happen
          %{^ref => _} ->
            exit(:unexpected_checkout)

          # The request is no longer active, do nothing
          %{} ->
            maybe_checkout(%{state | queue: queue})
        end

      {:empty, _queue} ->
        state
    end
  end

  defp maybe_checkout(command, mon_ref, deadline, {pid, ref} = from, state) do
    %{resources: resources, requests: requests, worker: worker, queue: queue} = state

    if past_deadline?(deadline) do
      requests = Map.delete(state.requests, ref)
      monitors = Map.delete(state.monitors, mon_ref)
      Process.demonitor(mon_ref, [:flush])
      maybe_checkout(%{state | requests: requests, monitors: monitors})
    else
      case :queue.out(resources) do
        {{:value, worker_server_state}, resources} ->
          case apply_callback(worker, :handle_checkout, [command, from, worker_server_state]) do
            {:ok, worker_client_state, worker_server_state} ->
              GenServer.reply({pid, ref}, worker_client_state)
              requests = Map.put(requests, ref, {pid, mon_ref, :state, worker_server_state})
              %{state | resources: resources, requests: requests}

            {:remove, reason} ->
              :ok = remove(reason, worker_server_state, state)
              maybe_checkout(command, mon_ref, deadline, from, %{state | resources: resources})

            other ->
              raise """
              unexpected return from #{inspect(worker)}.handle_checkout/3.

              Expected: {:ok, client_state, server_state} | {:remove, reason}
              Got: #{inspect(other)}
              """
          end

        {:empty, _} ->
          %{state | queue: :queue.in(from, queue)}
      end
    end
  end

  defp past_deadline?(deadline) when is_integer(deadline) do
    System.monotonic_time() >= deadline
  end

  defp past_deadline?(_), do: false

  defp remove(reason, worker_server_state, %{worker: worker}) do
    maybe_terminate(worker, reason, worker_server_state)
    schedule_init()
    :ok
  end

  defp maybe_terminate(worker, reason, worker_server_state) do
    if function_exported?(worker, :terminate, 2) do
      _ = apply_callback(worker, :terminate, [reason, worker_server_state])
    end
  end

  defp init_worker(worker, arg, resources, async) do
    do_init_worker(worker, arg, resources, async, fn fun, resources, async ->
      %{ref: ref, pid: pid} = Task.Supervisor.async(NimblePool.TaskSupervisor, fun)
      {resources, async |> Map.put(ref, pid) |> Map.put(pid, ref)}
    end)
  end

  defp force_init_worker(worker, arg, resources, async) do
    do_init_worker(worker, arg, resources, async, fn fun, resources, async ->
      {:queue.in(fun.(), resources), async}
    end)
  end

  defp do_init_worker(worker, arg, resources, async, handle_async_fun) do
    case apply_callback(worker, :init, [arg]) do
      {:ok, worker_state} ->
        {:queue.in(worker_state, resources), async}

      {:async, fun} when is_function(fun, 0) ->
        handle_async_fun.(fun, resources, async)

      {:remove, _reason} ->
        send(self(), {__MODULE__, :init})
        {resources, async}

      other ->
        raise """
        unexpected return from #{inspect(worker)}.init/1.

        Expected: {:ok, state} | {:async, (() -> state)}
        Got: #{inspect(other)}
        """
    end
  end

  defp schedule_init() do
    send(self(), {__MODULE__, :init})
  end

  defp apply_callback(worker, fun, args) do
    try do
      apply(worker, fun, args)
    catch
      kind, reason ->
        reason = Exception.normalize(kind, reason, __STACKTRACE__)

        Logger.error(
          [
            "Error during #{inspect(worker)}.#{fun}/#{length(args)} callback:\n"
            | Exception.format(kind, reason, __STACKTRACE__)
          ],
          crash_reason: {crash_reason(kind, reason), __STACKTRACE__}
        )

        {:remove, reason}
    end
  end

  defp crash_reason(:throw, value), do: {:nocatch, value}
  defp crash_reason(_, value), do: value
end
