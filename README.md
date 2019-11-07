# NimblePool

[Online Documentation](https://hexdocs.pm/nimble_pool).

<!-- MDOC !-->

`NimblePool` is a simple and small resource-pool implementation.

Pools in the Erlang VM, and therefore in Elixir, are generally process-based: they manages a group of processes. The downside of said pools is that when they have to manage resources, such as sockets or ports, the additional process leads to overhead.

In such pools, you usually end-up with two scenarios:

  a. You invoke the pool manager, which returns the pooled process, which performs the operation on the socket or port for you, returning you the reply. This approach is non-optimal because all of the data sent and returned by the resource needs to be copied between processes

  b. You invoke the pool manager, which returns the pooled process, which gives you access to the resource. Then you can act directly on the resource, avoiding the data copying, but you need to keep the state of the resource in sync with the process

NimblePool allows you to implement the scenario `b` without the addition of processes, which leads to a simpler and more efficient implementation. You should consider using NimblePool whenever you have to manage sockets, ports, or NIF resources and you want to perform **blocking** operations on them. For example, NimblePool is a good solution to manage processless HTTP 1 connections, ports that need to communicate with long-running programs on a single channel, etc.

NimblePool may not be a good option to manage processes. Also avoid using NimblePool to manage resources that support multiplexing, such as HTTP 2 connections (in fact, pools are not a good option to manage HTTP 2 connections in general).

## Examples

### Port-based example

```elixir
```

### HTTP1-based example

```elixir
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

Other nimble libraries by Plataformatec:

  * [NimbleCSV](https://github.com/plataformatec/nimble_csv) - simple and fast CSV parsing
  * [NimbleParsec](https://github.com/plataformatec/nimble_csv) - simple and fast parser combinators
  * [NimbleStrftime](https://github.com/plataformatec/nimble_strftime) - simple and fast strftime-based datetime formatter

## License

Copyright 2019 Plataformatec

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
