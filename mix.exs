defmodule Fable.MixProject do
  use Mix.Project

  @version "0.0.1-alpha.2"
  @source_url "https://github.com/CargoSense/fable"

  def project do
    [
      app: :fable,
      version: @version,
      elixir: "~> 1.10",
      start_permanent: Mix.env() == :prod,
      description: "Write simple, event-driven applications.",
      package: package(),
      deps: deps(),
      name: "Fable",
      source_url: @source_url,
      homepage_url: @source_url,
      elixirc_paths: elixirc_paths(Mix.env()),
      docs: &docs/0
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger]
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:ecto_sql, "~> 3.2"},
      {:jason, "~> 1.1", optional: true},
      {:postgrex, "~> 0.14"},
      {:telemetry, "~> 1.0"},
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false},
      {:ex_doc, ">= 0.0.0", only: :dev, runtime: false}
    ]
  end

  defp docs do
    [
      main: "Fable",
      extras: ["README.md"],
      formatters: ["html"],
      source_ref: "v#{@version}",
      source_url: @source_url
    ]
  end

  # Specifies which paths to compile per environment.
  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  defp package do
    [
      description: "Write simple, event-driven applications.",
      files: ["config", "lib", "mix.exs", "README*", ".formatter.exs"],
      licenses: ["MIT"],
      links: %{
        "GitHub" => @source_url,
        "Changelog" => "#{@source_url}/releases"
      },
      maintainers: ["Ben Wilson"]
    ]
  end
end
