# Peep.Storage.RustNIF

A [Peep](https://github.com/rkallos/peep) storage backend implemented as a Rust
NIF via [Rustler](https://github.com/rusterlium/rustler).

`Peep.Storage.RustNIF` implements the `Peep.Storage` behaviour, so it can be
passed as the `:storage` option to `Peep.start_link/1`:

```elixir
Peep.start_link(
  name: :my_peep,
  metrics: metrics,
  storage: {Peep.Storage.RustNIF, []}
)
```

## Development

`mix compile` builds the Rust crate under `native/peep_storage_rustler`.
It requires Cargo and OTP 29 (`enif_term_size`, NIF 2.18). The Nix flake supplies
Erlang, Elixir and Rust:

    direnv allow   # or: nix develop

`peep` is a path dependency (see `mix.exs`). Tests include Peep's shared storage
suite and `test/peep/storage/rust_nif_test.exs`:

    mix test
