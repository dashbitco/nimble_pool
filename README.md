# NimblePool

[Online Documentation](https://hexdocs.pm/nimble_pool).

<!-- MDOC !-->

`NimblePool` is a simple and small resource-pool implementation.

Pools in the Erlang VM, and therefore in Elixir, are generally process-based: they manages a group of processes. The downside of said pools is that when they have to manage resources, such as sockets or ports, the additional process leads to overhead.

In such pools, you usually end-up with two scenarios:

  * You invoke the pool manager, which returns the pooled process, which performs the operation on the socket or port for you, returning you the reply. This approach is non-optimal because all of the data sent and returned by the resource needs to be copied between processes

  * You invoke the pool manager, which returns the pooled process, which gives you access to the resource. Then you can act directly on the resource, avoiding the data copying, but you need to keep the state of the resource in sync with the process

NimblePool allows you to implement the second scenario without the addition of processes, which leads to a simpler and more efficient implementation. You should consider using NimblePool whenever you have to manage sockets, ports, or NIF resources and you want the client to perform one-off operations on them. For example, NimblePool is a good solution to manage HTTP 1 connections, ports that need to communicate with long-running programs, etc.

The downside of NimblePool is that, because all resources are under a single process, any resource management operation will happen on this single process, which is more likely to become a bottleneck. This can be addressed, however, by starting one NimblePool per scheduler and by doing scheduler-based dispatches.

NimblePool may not be a good option to manage processes. Also avoid using NimblePool to manage resources that support multiplexing, such as HTTP 2 connections (in fact, pools are not a good option to manage resources with multiplexing in general).

## Examples

To use `NimblePool`, you must define a module that implements the pool worker logic, outlined in the `NimblePool` behaviour.

### Port-based example

The example below keeps ports on the pool and check them out on every command. Please read the docs for `Port` before using the approach below, especially in regards to zombie ports.

```elixir
defmodule PortPool do
  @behaviour NimblePool

  @doc ~S"""
  Executes a given command against a port kept by the pool.

  First we start the port:

      iex> child = {NimblePool, worker: {PortPool, :cat}, name: PortPool}
      iex> Supervisor.start_link([child], strategy: :one_for_one)

  Now we can run commands against the pool of ports:

      iex> PortPool.command(PortPool, "hello\n")
      "hello\n"
      iex> PortPool.command(PortPool, "world\n")
      "world\n"

  """
  def command(pool, command, opts \\ []) do
    pool_timeout = Keyword.get(opts, :pool_timeout, 5000)
    receive_timeout = Keyword.get(opts, :receive_timeout, 15000)

    NimblePool.checkout!(pool, :checkout, fn {port, pool} ->
      send(port, {self(), {:command, command}})

      receive do
        {^port, {:data, data}} ->
          transfer_back(port, pool)
          {data, :done}
      after
        receive_timeout ->
          transfer_back(port, pool)
          exit(:receive_timeout)
      end
    end, pool_timeout)
  end

  defp transfer_back(port, pool) do
    send(port, {self(), {:connect, pool}})

    receive do
      {^port, :connected} -> :ok
    after
      timeout -> raise "did not transfer port"
    end
  end

  @impl NimblePool
  def init(:cat) do
    path = System.find_executable("cat")
    port = Port.open({:spawn_executable, path}, [:binary, args: ["-"]])
    {:ok, port}
  end

  @impl NimblePool
  # Transfer the port to the caller
  def handle_checkout(:checkout, {pid, _}, port) do
    send(port, {self(), {:connect, pid}})
    {:ok, {port, self()}, port}
  end

  @impl NimblePool
  # We got it back
  def handle_checkin(:done, _from, port) do
    {:ok, port}
  end

  @impl NimblePool
  # On terminate, effectively close it
  def terminate(_reason, port) do
    send(port, {self(), :close})
  end
end
```

### HTTP1-based example

The pool below uses [Mint](https://hexdocs.pm/mint) for HTTP1 connections.

```elixir
defmodule HTTP1Pool do
  @behaviour NimblePool

  @doc ~S"""
  Executes a given command against a connection kept by the pool.

  First we start the connection:

      child = {NimblePool, worker: {HTTP1Pool, {:http, "example.com", 80}}, name: HTTP1Pool}
      Supervisor.start_link([child], strategy: :one_for_one)

  Then we can access it:

      iex> HTTP1Pool.get(HTTP1Pool, "/foo")
      {:ok, %{...}}

  """
  def get(pool, path, opts \\ []) do
    pool_timeout = Keyword.get(opts, :pool_timeout, 5000)
    receive_timeout = Keyword.get(opts, :receive_timeout, 15000)

    NimblePool.checkout!(pool, :checkout, fn {conn, pool} ->
      try do
        {kind, conn, result_or_error} =
          with {:ok, conn, ref} <- Mint.HTTP1.request(conn, "GET", path, [], nil) do
            receive_response([], conn, ref, %{}, receive_timeout)
          end

        {:ok, conn} = Mint.HTTP1.controlling_process(conn, pool)
        {{kind, result_or_error}, conn}
      catch
        kind, reason ->
          _ = Mint.HTTP1.close(conn)
          :erlang.raise(kind, reason, __STACKTRACE__)
      end
    end, pool_timeout)
  end

  defp receive_response([], conn, ref, response, timeout) do
    {:ok, conn, entries} = Mint.HTTP1.recv(conn, 0, timeout)
    receive_response(entries, conn, ref, response, timeout)
  end

  defp receive_response([entry | entries], conn, ref, response, timeout) do
    case entry do
      {kind, ^ref, value} when kind in [:status, :headers] ->
        response = Map.put(response, kind, value)
        receive_response(entries, conn, ref, response, timeout)

      {:data, ^ref, data} ->
        response = Map.update(response, :data, data, &(&1 <> data))
        receive_response(entries, conn, ref, response, timeout)

      {:done, ^ref} ->
        {:ok, conn, response}

      {:error, ^ref, error} ->
        {:error, conn, error}
    end
  end

  @impl NimblePool
  def init({scheme, host, port}) do
    parent = self()

    async = fn ->
      # TODO: Add back-off
      {:ok, conn} = Mint.HTTP1.connect(scheme, host, port, [])
      {:ok, conn} = Mint.HTTP1.controlling_process(conn, parent)
      conn
    end

    {:async, async}
  end

  @impl NimblePool
  # Transfer the conn to the caller.
  # If we lost the connection, then we remove it to try again.
  def handle_checkout(:checkout, {pid, _}, conn) do
    with {:ok, conn} <- Mint.HTTP1.set_mode(conn, :passive),
         {:ok, conn} <- Mint.HTTP1.controlling_process(conn, pid) do
      {:ok, {conn, self()}, conn}
    else
      _ -> {:remove, :closed}
    end
  end

  @impl NimblePool
  # We got it back.
  def handle_checkin(conn, _from, _old_conn) do
    case Mint.HTTP1.set_mode(conn, :active) do
      {:ok, conn} -> {:ok, conn}
      {:error, _} -> {:remove, :closed}
    end
  end

  @impl NimblePool
  # If it is closed, drop it.
  def handle_info(message, conn) do
    case Mint.HTTP1.stream(conn, message) do
      {:ok, _, _} -> {:ok, conn}
      {:error, _, _, _} -> {:remove, :closed}
      :unknown -> {:ok, conn}
    end
  end

  @impl NimblePool
  # On terminate, effectively close it.
  # This will succeed even if it was already closed or if we don't own it.
  def terminate(_reason, conn) do
    Mint.HTTP1.close(conn)
  end
end
```

<!-- MDOC !-->

## Installation

Add `nimble_pool` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [{:nimble_pool, "~> 0.1"}]
end
```

## Nimble*

Other nimble libraries by Dashbit:

  * [NimbleCSV](https://github.com/dashbitco/nimble_csv) - simple and fast CSV parsing
  * [NimbleParsec](https://github.com/dashbitco/nimble_parsec) - simple and fast parser combinators

## License

Copyright 2019 Plataformatec \
Copyright 2020 Dashbit

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
