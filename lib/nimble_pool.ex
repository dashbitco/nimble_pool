defmodule NimblePool do
  @external_resource "README.md"
  @moduledoc "README.md"
             |> File.read!()
             |> String.split("<!-- MDOC !-->")
             |> Enum.fetch!(1)

  use GenServer
  require Logger

  @type from :: {pid, reference}
  @type init_arg :: term
  @type pool_state :: term
  @type worker_state :: term
  @type client_state :: term
  @type user_reason :: term

  @doc """
  Initializes the worker.

  It receives the worker argument passed to `start_link/1`. It must
  return `{:ok, worker_state}` or `{:async, fun}`, where the `fun`
  is a zero-arity function that must return the worker state.

  Note this callback is synchronous and therefore will block the pool.
  If you need to perform long initialization, consider using the
  `{:async, fun}` return type.
  """
  @callback init_worker(pool_state) ::
              {:ok, worker_state, pool_state} | {:async, (() -> worker_state), pool_state}

  @doc """
  Initializes the pool.

  It receives the worker argument passed to `start_link/1` and must
  return `{:ok, pool_state}` upon successful initialization,
  `:ignore` to exit normally, or `{:stop, reason}` to exit with `reason`
  and return `{:error, reason}`.

  This is a good place to perform a registration for example.

  It must return the `pool_state`. The `pool_state` is given to
  `init_worker`. By default, it simply returns the arguments given.

  This callback is optional.
  """
  @callback init_pool(init_arg) :: {:ok, pool_state} | :ignore | {:stop, reason :: any()}

  @doc """
  Checks a worker out.

  It receives `maybe_wrapped_command`. The `command` is given to the `checkout!/4`
  call and may optionally be wrapped by `c:handle_enqueue/2`. It must return either
  `{:ok, client_state, worker_state}`, `{:remove, reason, pool_state}`, or
  `{:skip, Exception.t(), pool_state}`.

  If `:remove` is returned, `NimblePool` will attempt to checkout another
  worker.

  If `:skip` is returned, `NimblePool` will skip the checkout, the client will
  raise the returned exception, and the worker will be left ready for the next
  checkout attempt.

  Note this callback is synchronous and therefore will block the pool.
  Avoid performing long work in here, instead do as much work as
  possible on the client.

  Once the connection is checked out, the worker won't receive any
  messages targetted to `c:handle_info/2`.
  """
  @callback handle_checkout(maybe_wrapped_command :: term, from, worker_state, pool_state) ::
              {:ok, client_state, worker_state, pool_state}
              | {:remove, user_reason, pool_state}
              | {:skip, Exception.t(), pool_state}

  @doc """
  Checks a worker in.

  It receives the `client_state`, returned by the `checkout!/4`
  anonymous function and it must return either `{:ok, worker_state}`
  or `{:remove, reason, pool_state}`.

  Note this callback is synchronous and therefore will block the pool.
  Avoid performing long work in here, instead do as much work as
  possible on the client.

  Once the connection is checked in, it may immediately be handed
  to another client, without traversing any of the messages in the
  pool inbox.

  This callback is optional.
  """
  @callback handle_checkin(client_state, from, worker_state, pool_state) ::
              {:ok, worker_state, pool_state} | {:remove, user_reason, pool_state}

  @doc """
  Handles update instruction from checked out worker.

  See `update/2` for more information.

  This callback is optional.
  """
  @callback handle_update(message :: term, worker_state, pool_state) ::
              {:ok, worker_state, pool_state}

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
  Executed by the pool, whenever a request to checkout a worker is enqueued.

  The `command` argument should be treated as an opaque value, but it can be
  wrapped with some data to be used in `c:handle_checkout/4`.

  It must return either `{:ok, maybe_wrapped_command, pool_state}` or
  `{:skip, Exception.t(), pool_state}` if checkout is to be skipped.

  Note this callback is synchronous and therefore will block the pool.
  Avoid performing long work in here.

  This callback is optional.
  """
  @callback handle_enqueue(command :: term, pool_state) ::
              {:ok, maybe_wrapped_command :: term, pool_state}
              | {:skip, Exception.t(), pool_state}

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
  @callback terminate_worker(
              :DOWN | :timeout | :throw | :error | :exit | user_reason,
              worker_state,
              pool_state
            ) ::
              {:ok, pool_state}

  @optional_callbacks init_pool: 1,
                      handle_checkin: 4,
                      handle_info: 2,
                      handle_enqueue: 2,
                      handle_update: 3,
                      terminate_worker: 3

  @doc """
  Defines a pool to be started under the supervision tree.

  It accepts the same options as `start_link/1` with the
  addition or `:restart` and `:shutdown` that control the
  "Child Specification".
  """
  def child_spec(opts)

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

    * `:pool_size` - how many workers in the pool. Defaults to 10.

    * `:lazy` - When `true`, workers are started lazily, only when necessary.
      Defaults to `false`.

  """
  def start_link(opts) do
    {{worker, arg}, opts} = Keyword.pop(opts, :worker)
    {pool_size, opts} = Keyword.pop(opts, :pool_size, 10)
    {lazy, opts} = Keyword.pop(opts, :lazy, false)

    unless is_atom(worker) do
      raise ArgumentError, "worker must be an atom, got: #{inspect(worker)}"
    end

    unless pool_size > 0 do
      raise ArgumentError, "pool_size must be more than 0, got: #{inspect(pool_size)}"
    end

    GenServer.start_link(__MODULE__, {worker, arg, pool_size, lazy}, opts)
  end

  @doc """
  Stops a pool.
  """
  def stop(pool, reason \\ :normal, timeout \\ :infinity) do
    GenServer.stop(pool, reason, timeout)
  end

  @doc """
  Checks out from the pool.

  It expects a command, which will be passed to the `c:handle_checkout/4`
  callback. The `c:handle_checkout/4` callback will return a client state,
  which is given to the `function`.

  The `function` receives two arguments, the pool reference and must return
  a two-element tuple, where the first element is the function return value,
  and the second element is the updated `client_state`, which will be given
  as the first argument to `c:handle_checkin/4`.

  `checkout!` also has an optional `timeout` value, this value will be applied
  to checkout operation itself. `checkin` happens asynchronously.
  """
  def checkout!(pool, command, function, timeout \\ 5_000) when is_function(function, 2) do
    # Reimplementation of gen.erl call to avoid multiple monitors.
    pid = GenServer.whereis(pool)

    unless pid do
      exit!(:noproc, :checkout, [pool])
    end

    ref = Process.monitor(pid)
    send_call(pid, ref, {:checkout, command, deadline(timeout)})

    receive do
      {^ref, {:skipped, exception}} ->
        raise exception

      {^ref, client_state} ->
        Process.demonitor(ref, [:flush])

        try do
          function.({pid, ref}, client_state)
        catch
          kind, reason ->
            send(pid, {__MODULE__, :cancel, ref, kind})
            :erlang.raise(kind, reason, __STACKTRACE__)
        else
          {result, client_state} ->
            send(pid, {__MODULE__, :checkin, ref, client_state})
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

  @doc """
  Sends an `update` instruction to the pool about the checked out worker.

  This must be called inside the `checkout!` callback with
  the `from` value given to `checkout`.

  This is useful to update the pool state before effectively
  checking the state in, which is handy when transferring
  resources that requires two steps.
  """
  def update({pid, ref}, command) do
    send(pid, {__MODULE__, :update, ref, command})
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

  defp exit!(reason, fun, args) do
    exit({reason, {__MODULE__, fun, args}})
  end

  ## Callbacks

  @impl true
  def init({worker, arg, pool_size, lazy}) do
    Process.flag(:trap_exit, true)
    _ = Code.ensure_loaded(worker)
    lazy = if lazy, do: pool_size, else: nil

    with {:ok, pool_state} <- do_init_pool(worker, arg) do
      {pool_state, resources, async} =
        if is_nil(lazy) do
          Enum.reduce(1..pool_size, {pool_state, :queue.new(), %{}}, fn
            _, {pool_state, resources, async} ->
              init_worker(worker, pool_state, resources, async)
          end)
        else
          {pool_state, :queue.new(), %{}}
        end

      state = %{
        worker: worker,
        queue: :queue.new(),
        requests: %{},
        monitors: %{},
        resources: resources,
        async: async,
        state: pool_state,
        lazy: lazy
      }

      {:ok, state}
    end
  end

  @impl true
  def handle_call({:checkout, command, deadline}, {pid, ref} = from, state) do
    %{requests: requests, monitors: monitors, worker: worker, state: pool_state} = state
    mon_ref = Process.monitor(pid)
    requests = Map.put(requests, ref, {pid, mon_ref, :command, command, deadline})
    monitors = Map.put(monitors, mon_ref, ref)
    state = %{state | requests: requests, monitors: monitors}

    case handle_enqueue(worker, command, pool_state) do
      {:ok, command, pool_state} ->
        {:noreply, maybe_checkout(command, mon_ref, deadline, from, %{state | state: pool_state})}

      {:skip, exception, pool_state} ->
        state = remove_request(%{state | state: pool_state}, ref, mon_ref)
        {:reply, {:skipped, exception}, state}
    end
  end

  @impl true
  def handle_info({__MODULE__, :update, ref, command}, state) do
    %{requests: requests, state: pool_state, worker: worker} = state

    case requests do
      %{^ref => {pid, mon_ref, :state, worker_state}} ->
        {:ok, worker_state, pool_state} = worker.handle_update(command, worker_state, pool_state)
        requests = Map.put(requests, ref, {pid, mon_ref, :state, worker_state})
        {:noreply, %{state | requests: requests, state: pool_state}}

      %{} ->
        exit(:unexpected_precheckin)
    end
  end

  @impl true
  def handle_info({__MODULE__, :checkin, ref, worker_client_state}, state) do
    %{requests: requests, resources: resources, worker: worker, state: pool_state} = state

    case requests do
      %{^ref => {pid, mon_ref, :state, worker_server_state}} ->
        checkin =
          if function_exported?(worker, :handle_checkin, 4) do
            args = [worker_client_state, {pid, ref}, worker_server_state, pool_state]
            apply_worker_callback(pool_state, worker, :handle_checkin, args)
          else
            {:ok, worker_server_state, pool_state}
          end

        {resources, state} =
          case checkin do
            {:ok, worker_server_state, pool_state} ->
              {:queue.in(worker_server_state, resources), %{state | state: pool_state}}

            {:remove, reason, pool_state} ->
              {resources,
               remove_worker(reason, worker_server_state, %{state | state: pool_state})}
          end

        state = remove_request(state, ref, mon_ref)
        {:noreply, maybe_checkout(%{state | resources: resources})}

      %{} ->
        exit(:unexpected_checkin)
    end
  end

  @impl true
  def handle_info({__MODULE__, :cancel, ref, reason}, state) do
    cancel_request_ref(ref, reason, state)
  end

  @impl true
  def handle_info({__MODULE__, :init_worker}, state) do
    %{async: async, resources: resources, worker: worker, state: pool_state} = state
    {pool_state, resources, async} = init_worker(worker, pool_state, resources, async)
    {:noreply, maybe_checkout(%{state | async: async, resources: resources, state: pool_state})}
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
  def handle_info({:EXIT, pid, _reason} = exit, state) do
    %{async: async} = state

    case async do
      %{^pid => _} -> {:noreply, %{state | async: Map.delete(async, pid)}}
      %{} -> maybe_handle_info(exit, state)
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
  def handle_info(msg, state) do
    maybe_handle_info(msg, state)
  end

  @impl true
  def terminate(reason, %{resources: resources} = state) do
    for worker_server_state <- :queue.to_list(resources) do
      maybe_terminate_worker(reason, worker_server_state, state)
    end

    :ok
  end

  defp do_init_pool(worker, arg) do
    if function_exported?(worker, :init_pool, 1) do
      worker.init_pool(arg)
    else
      {:ok, arg}
    end
  end

  defp remove_async_ref(ref, state) do
    %{async: async, resources: resources, worker: worker, state: pool_state} = state

    # If an async worker failed to start, we try to start another one
    # immediately, even if the pool is lazy, as we assume there is an
    # immediate need for this resource.
    {pool_state, resources, async} =
      init_worker(worker, pool_state, resources, Map.delete(async, ref))

    {:noreply, %{state | resources: resources, async: async, state: pool_state}}
  end

  defp cancel_request_ref(ref, reason, %{requests: requests} = state) do
    case requests do
      # Exited or timed out before we could serve it
      %{^ref => {_, mon_ref, :command, _command, _deadline}} ->
        {:noreply, remove_request(state, ref, mon_ref)}

      # Exited or errored during client processing
      %{^ref => {_, mon_ref, :state, worker_server_state}} ->
        state = remove_request(state, ref, mon_ref)
        {:noreply, remove_worker(reason, worker_server_state, state)}

      %{} ->
        exit(:unexpected_remove)
    end
  end

  defp maybe_handle_info(msg, state) do
    %{resources: resources, worker: worker} = state

    if function_exported?(worker, :handle_info, 2) do
      {resources, state} =
        Enum.reduce(:queue.to_list(resources), {:queue.new(), state}, fn
          worker_server_state, {resources, state} ->
            case apply_worker_callback(worker, :handle_info, [msg, worker_server_state]) do
              {:ok, worker_server_state} ->
                {:queue.in(worker_server_state, resources), state}

              {:remove, reason} ->
                {resources, remove_worker(reason, worker_server_state, state)}
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
    if past_deadline?(deadline) do
      state = remove_request(state, ref, mon_ref)
      maybe_checkout(state)
    else
      %{resources: resources, requests: requests, worker: worker, queue: queue, state: pool_state} =
        state = init_worker_if_lazy_and_empty(state)

      case :queue.out(resources) do
        {{:value, worker_server_state}, resources} ->
          args = [command, from, worker_server_state, pool_state]

          case apply_worker_callback(pool_state, worker, :handle_checkout, args) do
            {:ok, worker_client_state, worker_server_state, pool_state} ->
              GenServer.reply({pid, ref}, worker_client_state)

              requests = Map.put(requests, ref, {pid, mon_ref, :state, worker_server_state})
              %{state | resources: resources, requests: requests, state: pool_state}

            {:remove, reason, pool_state} ->
              state = remove_worker(reason, worker_server_state, %{state | state: pool_state})
              maybe_checkout(command, mon_ref, deadline, from, %{state | resources: resources})

            {:skip, exception, pool_state} ->
              GenServer.reply({pid, ref}, {:skipped, exception})
              remove_request(%{state | state: pool_state}, ref, mon_ref)

            other ->
              raise """
              unexpected return from #{inspect(worker)}.handle_checkout/4.

              Expected: {:ok, client_state, server_state, pool_state} | {:remove, reason, pool_state} | {:skip, Exception.t(), pool_state}
              Got: #{inspect(other)}
              """
          end

        {:empty, _} ->
          %{state | queue: :queue.in(from, queue)}
      end
    end
  end

  defp init_worker_if_lazy_and_empty(%{lazy: nil} = state), do: state

  defp init_worker_if_lazy_and_empty(%{lazy: lazy, resources: resources} = state) do
    if lazy > 0 and :queue.is_empty(resources) do
      %{async: async, worker: worker, state: pool_state} = state
      {pool_state, resources, async} = init_worker(worker, pool_state, resources, async)
      %{state | async: async, resources: resources, state: pool_state, lazy: lazy - 1}
    else
      state
    end
  end

  defp past_deadline?(deadline) when is_integer(deadline) do
    System.monotonic_time() >= deadline
  end

  defp past_deadline?(:infinity), do: false

  defp remove_worker(reason, worker_server_state, state) do
    state = maybe_terminate_worker(reason, worker_server_state, state)

    if lazy = state.lazy do
      %{state | lazy: lazy + 1}
    else
      schedule_init()
      state
    end
  end

  defp maybe_terminate_worker(reason, worker_server_state, state) do
    %{worker: worker, state: pool_state} = state

    if function_exported?(worker, :terminate_worker, 3) do
      args = [reason, worker_server_state, pool_state]

      case apply_worker_callback(worker, :terminate_worker, args) do
        {:ok, pool_state} ->
          %{state | state: pool_state}

        {:remove, _reason} ->
          state

        other ->
          raise """
          unexpected return from #{inspect(worker)}.terminate_worker/3.

          Expected:

              {:ok, pool_state}

          Got: #{inspect(other)}
          """
      end
    else
      state
    end
  end

  defp init_worker(worker, pool_state, resources, async) do
    case apply_worker_callback(worker, :init_worker, [pool_state]) do
      {:ok, worker_state, pool_state} ->
        {pool_state, :queue.in(worker_state, resources), async}

      {:async, fun, pool_state} when is_function(fun, 0) ->
        %{ref: ref, pid: pid} = Task.Supervisor.async(NimblePool.TaskSupervisor, fun)
        {pool_state, resources, async |> Map.put(ref, pid) |> Map.put(pid, ref)}

      {:remove, _reason} ->
        send(self(), {__MODULE__, :init_worker})
        {pool_state, resources, async}

      other ->
        raise """
        unexpected return from #{inspect(worker)}.init_worker/1.

        Expected:

            {:ok, worker_state, pool_state}
            | {:async, (() -> worker_state), pool_state}

        Got: #{inspect(other)}
        """
    end
  end

  defp schedule_init() do
    send(self(), {__MODULE__, :init_worker})
  end

  defp apply_worker_callback(worker, fun, args) do
    do_apply_worker_callback(worker, fun, args, &{:remove, &1})
  end

  defp apply_worker_callback(pool_state, worker, fun, args) do
    do_apply_worker_callback(worker, fun, args, &{:remove, &1, pool_state})
  end

  defp do_apply_worker_callback(worker, fun, args, catch_fun) do
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

        catch_fun.(reason)
    end
  end

  defp crash_reason(:throw, value), do: {:nocatch, value}
  defp crash_reason(_, value), do: value

  defp remove_request(pool_state, ref, mon_ref) do
    requests = Map.delete(pool_state.requests, ref)
    monitors = Map.delete(pool_state.monitors, mon_ref)
    Process.demonitor(mon_ref, [:flush])
    %{pool_state | requests: requests, monitors: monitors}
  end

  defp handle_enqueue(worker, command, pool_state) do
    if function_exported?(worker, :handle_enqueue, 2) do
      worker.handle_enqueue(command, pool_state)
    else
      {:ok, command, pool_state}
    end
  end
end
