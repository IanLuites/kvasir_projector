defmodule Kvasir.Projector.MixProject do
  use Mix.Project
  @version "0.0.1-proto0"

  def project do
    [
      app: :kvasir_projector,
      description: "Kvasir projector extension to allow event projection.",
      version: @version,
      elixir: "~> 1.7",
      build_embedded: Mix.env() == :prod,
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      package: package(),

      # Testing
      test_coverage: [tool: ExCoveralls],
      preferred_cli_env: [
        coveralls: :test,
        "coveralls.detail": :test,
        "coveralls.post": :test,
        "coveralls.html": :test
      ],
      # dialyzer: [ignore_warnings: "dialyzer.ignore-warnings", plt_add_deps: true],

      # Docs
      name: "kvasir_projector",
      source_url: "https://github.com/IanLuites/kvasir_projector",
      homepage_url: "https://github.com/IanLuites/kvasir_projector",
      docs: [
        main: "readme",
        extras: ["README.md"]
      ]
    ]
  end

  def package do
    [
      name: :kvasir_projector,
      maintainers: ["Ian Luites"],
      licenses: ["MIT"],
      files: [
        # Elixir
        "lib/projector",
        "lib/projector.ex",
        "mix.exs",
        "README*",
        "LICENSE*"
      ],
      links: %{
        "GitHub" => "https://github.com/IanLuites/kvasir_projector"
      }
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger],
      mod: {Kvasir.Projector.Application, []}
    ]
  end

  defp deps do
    [
      # {:kvasir, path: "../core", optional: true},
      {:ex_doc, ">= 0.0.0", only: :dev}
    ]
  end
end
