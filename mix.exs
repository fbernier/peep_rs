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
      # Forked for `Term::hash_internal/1`: upstream truncates
      # `ERL_NIF_INTERNAL_HASH` to 32 bits, and the shard maps are keyed by it.
      {:rustler,
       git: "https://github.com/rkallos/rustler.git",
       branch: "fix/internal-hash-64bit",
       sparse: "rustler_mix",
       runtime: false}
    ]
  end
end
