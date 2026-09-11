defmodule Peep.Storage.RustNIF.MixProject do
  use Mix.Project

  def project do
    [
      app: :peep_rs,
      version: "0.1.0",
      elixir: "~> 1.20",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  def application do
    [
      extra_applications: [:logger]
    ]
  end

  defp deps do
    [
      {:peep, path: "../../elixir/peep-rust"},
      # Match Cargo.toml's hash fix and override Peep's Hex dependency.
      {:rustler,
       git: "https://github.com/rusterlium/rustler.git",
       ref: "0ba085adabd26632506aeeb3fae7534c9eff96d1",
       sparse: "rustler_mix",
       runtime: false,
       override: true}
    ]
  end
end
