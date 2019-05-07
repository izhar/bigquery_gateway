defmodule BigqueryGateway.MixProject do
  use Mix.Project

  def project do
    [
      app: :bigquery_gateway,
      version: "0.1.0",
      elixir: "~> 1.7",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger],
      mod: {BigqueryGateway.Application, []}
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:google_api_big_query, "~> 0.6.0"},
      {:goth, "~> 1.0.1"},
      {:poison, "~> 3.1"},
      {:httpoison, "~> 1.5.1"},
      {:gen_stage, "~> 0.14"}
    ]
  end
end
