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

`mix compile` builds the Rust crate under `native/peep_storage_rustler`, so a
Rust toolchain (`cargo`) must be on `PATH`. A Nix flake providing Erlang,
Elixir, and the Rust toolchain is included:

    direnv allow   # or: nix develop

`peep` is a path dependency (see `mix.exs`). The test suite runs Peep's shared
storage conformance tests (`test/shared/storage_test.exs`, shipped with the peep
dependency) against the Rust NIF backend, plus backend-specific tests in
`test/peep/storage/rust_nif_test.exs`:

    mix test
