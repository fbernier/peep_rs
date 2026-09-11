# Backend-only timings in ns/op; tag construction and registration are excluded.
# Compare medians from interleaved runs of separate revisions.
# MIX_ENV=prod ELIXIR_ERL_OPTIONS="+S 4:4 +SDcpu 4:4 +sbt db" \
#   mix run bench/storage.exs [--case shared] [--scale 3]

defmodule PeepStorageBench do
  alias Peep.Storage.RustNIF, as: NIF
  alias Telemetry.Metrics

  def run(mode, scale) do
    cases =
      ~w(hot_counter hot_one hot_empty hot_mixed hot_float cold scrape20 scrape105 shared size_cached prune_none prune_half prune_all)

    unless mode == "all" or mode in cases, do: raise(ArgumentError, "unknown case: #{mode}")

    for name <- if(mode == "all", do: cases, else: [mode]) do
      :erlang.garbage_collect()
      run_case(name, scale)
    end
  end

  defp metric(:distribution, id, buckets) do
    boundaries = Enum.map(1..buckets, &(&1 * 1.5))

    Metrics.distribution("perf.dist#{id}")
    |> Map.put(:peep_bucket_boundaries, boundaries)
    |> Map.put(:peep_bucket_labels, Enum.map(boundaries, &to_string/1) ++ [:infinity])
  end

  defp metric(:counter, id, _), do: Metrics.counter("perf.counter#{id}")
  defp metric(:sum, id, _), do: Metrics.sum("perf.sum#{id}")
  defp metric(:last_value, id, _), do: Metrics.last_value("perf.gauge#{id}")

  defp fixture(kinds, buckets, cardinality, float? \\ false) do
    metrics =
      kinds
      |> Enum.with_index()
      |> Enum.map(fn {kind, id} -> metric(kind, id, buckets) end)
      |> List.to_tuple()

    batch =
      for {kind, id} <- Enum.with_index(kinds),
          do:
            {id, elem(metrics, id), if(kind == :distribution and float?, do: 27.25, else: 27), 0}

    tags =
      for id <- 1..cardinality, do: {%{method: :get, route: "/items/#{id}", status: rem(id, 2)}}

    {metrics, batch, List.to_tuple(tags)}
  end

  defp new(metrics) do
    s = NIF.new([])
    :ok = NIF.nif_register_metrics(s, metrics)
    s
  end

  defp insert(0, _, _, _), do: :ok

  defp insert(n, s, tags, batch) do
    :ok = NIF.insert_metrics(s, elem(tags, rem(n - 1, tuple_size(tags))), batch)
    insert(n - 1, s, tags, batch)
  end

  defp repeat(0, _), do: :ok

  defp repeat(n, fun) do
    fun.()
    repeat(n - 1, fun)
  end

  defp measured(name, n, fun, checksum) do
    :erlang.garbage_collect()
    started = System.monotonic_time(:nanosecond)
    fun.()
    ns = System.monotonic_time(:nanosecond) - started
    IO.puts("RESULT\t#{name}\t#{Float.round(ns / n, 2)}\t#{checksum.()}")
  end

  defp run_case("hot_" <> kind = name, scale) do
    kinds =
      case kind do
        key when key in ["counter", "one", "empty"] -> [:counter]
        "mixed" -> [:counter, :sum, :last_value, :distribution]
        "float" -> [:distribution]
      end

    cardinality = if kind in ["one", "empty"], do: 1, else: 200
    {metrics, batch, tags} = fixture(kinds, 32, cardinality, kind == "float")
    tags = if kind == "empty", do: {{%{}}}, else: tags
    s = new(metrics)
    insert(10_000, s, tags, batch)
    n = 1_000_000 * scale

    measured(name, n, fn -> insert(n, s, tags, batch) end, fn ->
      :erlang.phash2(NIF.nif_get_all_metrics(s, metrics))
    end)
  end

  defp run_case("cold", scale) do
    {metrics, batch, tags} = fixture([:counter, :sum, :last_value, :distribution], 20, 20_000)

    for _ <- 1..(3 * scale) do
      s = new(metrics)

      measured("cold", tuple_size(tags), fn -> insert(tuple_size(tags), s, tags, batch) end, fn ->
        NIF.storage_size(s).size
      end)
    end
  end

  defp run_case("scrape" <> count = name, scale) do
    {metrics, batch, tags} = fixture([:distribution], String.to_integer(count), 5_000)
    s = new(metrics)
    insert(tuple_size(tags), s, tags, batch)
    repeat(5, fn -> NIF.nif_get_all_metrics(s, metrics) end)

    measured(
      name,
      60 * scale,
      fn -> repeat(60 * scale, fn -> NIF.nif_get_all_metrics(s, metrics) end) end,
      fn -> :erlang.phash2(NIF.nif_get_all_metrics(s, metrics)) end
    )
  end

  defp run_case("shared", scale) do
    {metrics, batch, tags} = fixture(List.duplicate(:counter, 16), 20, 5_000)
    s = new(metrics)

    workers =
      for scheduler <- 1..min(4, :erlang.system_info(:schedulers_online)) do
        :erlang.spawn_opt(fn -> insert(tuple_size(tags), s, tags, batch) end, [
          :monitor,
          {:scheduler, scheduler}
        ])
      end

    for {pid, ref} <- workers do
      receive do
        {:DOWN, ^ref, :process, ^pid, :normal} -> :ok
        {:DOWN, ^ref, :process, ^pid, reason} -> raise inspect(reason)
      after
        30_000 -> raise "seed timeout"
      end
    end

    repeat(3, fn -> NIF.nif_get_all_metrics(s, metrics) end)

    measured(
      "shared",
      20 * scale,
      fn -> repeat(20 * scale, fn -> NIF.nif_get_all_metrics(s, metrics) end) end,
      fn -> :erlang.phash2(NIF.nif_get_all_metrics(s, metrics)) end
    )
  end

  defp run_case("size_cached", scale) do
    {metrics, batch, tags} = fixture([:counter, :sum, :last_value, :distribution], 20, 20_000)
    s = new(metrics)
    insert(tuple_size(tags), s, tags, batch)
    measured("size_first", 1, fn -> NIF.storage_size(s) end, fn -> NIF.storage_size(s).size end)
    repeat(5, fn -> NIF.storage_size(s) end)

    measured(
      "size_cached",
      200 * scale,
      fn -> repeat(200 * scale, fn -> NIF.storage_size(s) end) end,
      fn -> NIF.storage_size(s).size end
    )
  end

  defp run_case("prune_" <> mode = name, scale) do
    {metrics, batch, tags} = fixture([:counter], 20, 10_000)

    {pattern, survivors} =
      case mode do
        "none" -> {%{status: -1}, 10_000}
        "half" -> {%{status: 1}, 5_000}
        "all" -> {%{}, 0}
      end

    for _ <- 1..(5 * scale) do
      s = new(metrics)
      insert(tuple_size(tags), s, tags, batch)

      measured(name, 1, fn -> :ok = NIF.prune_tags(s, [pattern]) end, fn ->
        size = NIF.storage_size(s).size
        if size != survivors, do: raise("prune count mismatch")
        size
      end)
    end
  end
end

{opts, rest, invalid} =
  OptionParser.parse(System.argv(), strict: [case: :string, scale: :integer])

if rest != [] or invalid != [], do: raise(ArgumentError, "use --case CASE and --scale N")
mode = Keyword.get(opts, :case, "all")
scale = Keyword.get(opts, :scale, 1)
unless scale in 1..100, do: raise(ArgumentError, "scale must be in 1..100")
IO.inspect(:erlang.system_info(:scheduler_bindings), label: "scheduler_bindings")

{pid, ref} =
  :erlang.spawn_opt(fn -> PeepStorageBench.run(mode, scale) end, [:monitor, {:scheduler, 1}])

receive do
  {:DOWN, ^ref, :process, ^pid, :normal} -> :ok
  {:DOWN, ^ref, :process, ^pid, reason} -> raise inspect(reason)
after
  120_000 * scale -> raise "benchmark timeout"
end
