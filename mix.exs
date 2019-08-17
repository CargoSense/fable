defmodule Fable.MixProject do
  use Mix.Project

  @version "0.0.1-alpha.0"

  def project do
    [
      app: :fable,
      version: @version,
      elixir: "~> 1.7",
      start_permanent: Mix.env() == :prod,
      description: "Write simple, event driven applications",
      deps: deps(),
      package: package(),
      elixirc_paths: elixirc_paths(Mix.env()),
      docs: [
        main: "Fable",
        source_ref: "v#{@version}",
        source_url: "https://github.com/cargosense/fable"
      ]
    ]
  end

  defp package do
    [
      description: "Write simple, event driven applications",
      files: ["lib", "config", "mix.exs", "README*", ".formatter.exs"],
      maintainers: ["Ben Wilson"],
      licenses: ["MIT"],
      links: %{github: "https://github.com/CargoSense/fable"}
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger]
    ]
  end

  # Specifies which paths to compile per environment.
  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:ex_doc, "~> 0.21", only: [:dev]},
      {:ecto, "~> 3.0"},
      {:ecto_sql, "~> 3.0"},
      {:postgrex, "~> 0.14.0"},
      {:jason, "~> 1.1", optional: true}
    ]
  end
end
