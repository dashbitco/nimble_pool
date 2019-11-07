defmodule NimblePool.MixProject do
  use Mix.Project

  def project do
    [
      app: :nimble_pool,
      version: "0.1.0",
      elixir: "~> 1.7",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  def application do
    [
      mod: {NimblePool.Application, []},
      extra_applications: [:logger]
    ]
  end

  defp deps do
    []
  end
end
