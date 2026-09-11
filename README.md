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

## Storage semantics

Integer measurements and boundaries retain their precision. Distribution sums
round floats and reject contributions outside signed 64-bit range. Accumulators
wrap on overflow.

Last-value observations share an event timestamp. Ties compare values
numerically, then prefer floats over equal integers and positive over negative
zero. Unlike Erlang `max/2`, this fixes the result's representation.

Rejected samples retain no tags or series; successful batch prefixes remain.
Errors raise `{:peep_storage_error, reason, detail}`. Match `reason`, not the
diagnostic text; `:invariant_violation` indicates an internal inconsistency.

Inserts run on normal schedulers unless the shard is busy, then continue on a
dirty CPU scheduler using the same shard. Scans still contend with updates,
but lock waits do not block normal schedulers.

Size reports cache tag-term sizes until membership changes. The first report
after adding or pruning tags scans them; repeated reports only visit metrics.

## Development

`mix compile` builds the Rust crate under `native/peep_storage_rustler`.
It requires Cargo and OTP 29 (`enif_term_size`, NIF 2.18). The Nix flake supplies
Erlang, Elixir and Rust:

    direnv allow   # or: nix develop

`peep` is a path dependency (see `mix.exs`). Tests include Peep's shared storage
suite and `test/peep/storage/rust_nif_test.exs`:

    mix test

### Contention benchmark

Compare idle insertion with concurrent scrape, memory-report and prune scans.
The benchmark reports throughput, latency and scheduler heartbeat delays, and
checks for lost or duplicated observations.

    MIX_ENV=prod ELIXIR_ERL_OPTIONS="+S 2:2 +SDcpu 2:2" \
      mix run bench/contention.exs --cardinality 50000 --duration-ms 2000

Scans run continuously. Compare repeated runs with identical dependencies and
runtime options. Call latency includes scheduling, not just lock waits.

### Storage benchmarks

Measure insert, scrape, size-report and prune costs separately. The worker is
pinned to scheduler 1; the shared-series case seeds four schedulers.

    MIX_ENV=prod ELIXIR_ERL_OPTIONS="+S 4:4 +SDcpu 4:4 +sbt db" \
      mix run bench/storage.exs --case shared --scale 3

Omit `--case` to run all cases. Results are ns/op; compare medians of interleaved
runs. `size_first` and `size_cached` distinguish cache population from reuse.
