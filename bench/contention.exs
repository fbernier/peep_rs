# MIX_ENV=prod ELIXIR_ERL_OPTIONS="+S 2:2 +SDcpu 2:2" \
#   mix run bench/contention.exs --cardinality 50000 --duration-ms 2000
#
# Compare repeated runs with identical dependencies and runtime options.
# Writer and heartbeat are scheduler-pinned, not CPU-pinned. Latency includes
# scheduling; p50/p99 use 1-us upper bounds with overflow above 100 ms.
# Throughput includes bookkeeping; heartbeat measures lateness of 1-ms wakeups.
# Nonmatching prune scans preserve cardinality.

defmodule PeepContentionBench do
  alias Peep.Storage.RustNIF, as: NIF

  @histogram_limit 100_000
  @setup_timeout_ms 120_000
  @drain_timeout_ms 120_000

  def run(args) do
    {opts, rest, invalid} =
      OptionParser.parse(args, strict: [cardinality: :integer, duration_ms: :integer])

    if rest != [] or invalid != [], do: raise(ArgumentError, "invalid options: #{inspect(args)}")
    cardinality = bounded_option(opts, :cardinality, 50_000, 1, 1_000_000)
    duration_ms = bounded_option(opts, :duration_ms, 2_000, 100, 60_000)
    scan_scheduler = min(2, :erlang.system_info(:schedulers_online))

    IO.puts(
      "OTP #{:erlang.system_info(:otp_release)} / ERTS #{:erlang.system_info(:version)} / Elixir #{System.version()}"
    )

    IO.puts(
      "normal schedulers=#{:erlang.system_info(:schedulers_online)}, dirty CPU schedulers=#{:erlang.system_info(:dirty_cpu_schedulers_online)}"
    )

    IO.puts(
      "writer + heartbeat scheduler=1, scan caller scheduler=#{scan_scheduler}; cardinality=#{cardinality}, phase_ms=#{duration_ms}"
    )

    IO.puts(
      "MIX_ENV=#{Mix.env()} ELIXIR_ERL_OPTIONS=#{inspect(System.get_env("ELIXIR_ERL_OPTIONS"))} ERL_FLAGS=#{inspect(System.get_env("ERL_FLAGS"))}"
    )

    IO.puts(
      "Insertion end-to-end latency in us (p50/p99 histogram upper bounds); heartbeat lateness in us. No lock-wait attribution."
    )

    if scan_scheduler == 1 do
      IO.puts(
        "WARNING: only one normal scheduler; scan initiation shares the writer scheduler. Prefer +S 2:2."
      )
    end

    for {phase, scans} <- [
          idle: [],
          scrape: [:scrape],
          storage_size: [:storage_size],
          prune: [:prune],
          combined: [:scrape, :storage_size, :prune]
        ] do
      run_phase(phase, scans, scan_scheduler, cardinality, duration_ms)
      # Release native storage between phases.
      :erlang.garbage_collect()
    end
  end

  defp bounded_option(opts, key, default, minimum, maximum) do
    value = Keyword.get(opts, key, default)

    unless is_integer(value) and value >= minimum and value <= maximum do
      raise ArgumentError, "#{key} must be an integer in #{minimum}..#{maximum}"
    end

    value
  end

  defp run_phase(phase, scans, scan_scheduler, cardinality, duration_ms) do
    metrics = {Telemetry.Metrics.counter("contention.count", tags: [:id])}
    storage = NIF.new([])
    :ok = NIF.register_metrics(storage, metrics)
    parent = self()

    workers =
      [
        spawn_worker(parent, :writer, 1, fn -> writer(parent, storage, metrics, cardinality) end),
        spawn_worker(parent, :heartbeat, 1, fn -> heartbeat(parent) end)
      ] ++
        Enum.map(scans, fn kind ->
          spawn_worker(parent, kind, scan_scheduler, fn ->
            {start, deadline} = ready(parent)
            sleep_until(start)
            scan_loop(kind, storage, metrics, deadline, 0, 0, nil)
          end)
        end)

    workers = Map.new(workers)

    try do
      await_ready(Map.keys(workers), workers, now() + @setup_timeout_ms * 1_000_000)
      start = now() + 100_000_000
      deadline = start + duration_ms * 1_000_000
      Enum.each(workers, fn {pid, _} -> send(pid, {:start, start, deadline}) end)
      results = await_results(workers, %{}, deadline + @drain_timeout_ms * 1_000_000)
      series = NIF.nif_get_all_metrics(storage, metrics) |> Map.fetch!(elem(metrics, 0))

      unless map_size(series) == cardinality and
               Enum.sum(Map.values(series)) == cardinality + results.writer.count do
        raise "#{phase}: concurrent work lost or duplicated observations"
      end

      report(phase, scans, results)
    after
      Enum.each(workers, fn {pid, {ref, _role}} ->
        Process.exit(pid, :kill)
        Process.demonitor(ref, [:flush])
      end)
    end
  end

  defp spawn_worker(parent, role, scheduler, fun) do
    {pid, ref} =
      :erlang.spawn_opt(
        fn -> send(parent, {:result, self(), fun.()}) end,
        [:monitor, {:scheduler, scheduler}]
      )

    {pid, {ref, role}}
  end

  defp ready(parent) do
    send(parent, {:ready, self()})

    receive do
      {:start, start, deadline} -> {start, deadline}
    end
  end

  defp await_ready([], _workers, _timeout), do: :ok

  defp await_ready(pending, workers, timeout) do
    receive do
      {:ready, pid} -> await_ready(List.delete(pending, pid), workers, timeout)
      {:DOWN, _ref, :process, pid, reason} -> worker_failed(workers, pid, reason)
    after
      remaining_ms(timeout) -> raise "timed out preparing benchmark workers"
    end
  end

  defp await_results(workers, results, _timeout) when map_size(workers) == 0, do: results

  defp await_results(workers, results, timeout) do
    receive do
      {:result, pid, result} ->
        {_ref, role} = Map.fetch!(workers, pid)
        await_results(workers, Map.put(results, role, result), timeout)

      {:DOWN, _ref, :process, pid, :normal} ->
        {_ref, role} = Map.fetch!(workers, pid)
        unless Map.has_key?(results, role), do: raise("#{role} exited without a result")
        await_results(Map.delete(workers, pid), results, timeout)

      {:DOWN, _ref, :process, pid, reason} ->
        worker_failed(workers, pid, reason)
    after
      remaining_ms(timeout) ->
        raise "benchmark workers failed to drain within #{@drain_timeout_ms} ms"
    end
  end

  defp worker_failed(workers, pid, reason) do
    {_ref, role} = Map.fetch!(workers, pid)
    raise "#{role} worker crashed: #{inspect(reason)}"
  end

  defp writer(parent, storage, metrics, cardinality) do
    # Older backends return a scheduler-specific handle.
    resolved = NIF.resolve(storage)
    batch = [{0, elem(metrics, 0), 1, 0}]

    tags =
      for id <- 1..cardinality do
        tags = {%{id: id}}
        :ok = NIF.insert_metrics(resolved, tags, batch)
        tags
      end
      |> List.to_tuple()

    histogram = :counters.new(@histogram_limit + 1, [])
    {start, deadline} = ready(parent)
    sleep_until(start)
    started = now()
    {count, slow, maximum} = write_loop(resolved, batch, tags, 0, deadline, histogram, 0, 0, 0)
    stopped = now()

    %{
      count: count,
      slow: slow,
      maximum: maximum,
      histogram: histogram,
      started: started,
      stopped: stopped
    }
  end

  defp write_loop(resolved, batch, tags, index, deadline, histogram, count, slow, maximum) do
    tag = elem(tags, index)
    before = now()

    if before >= deadline do
      {count, slow, maximum}
    else
      :ok = NIF.insert_metrics(resolved, tag, batch)
      elapsed = now() - before
      bin = min(@histogram_limit + 1, max(1, div(elapsed + 999, 1_000)))
      :counters.add(histogram, bin, 1)
      index = if index + 1 == tuple_size(tags), do: 0, else: index + 1
      slow = if elapsed > 1_000_000, do: slow + 1, else: slow

      write_loop(
        resolved,
        batch,
        tags,
        index,
        deadline,
        histogram,
        count + 1,
        slow,
        max(maximum, elapsed)
      )
    end
  end

  defp heartbeat(parent) do
    {start, deadline} = ready(parent)
    beat_loop(start + 1_000_000, deadline, 0, 0)
  end

  defp beat_loop(due, deadline, maximum, count) do
    sleep_until(due)
    woke = now()
    maximum = max(maximum, woke - due)

    if woke >= deadline do
      %{maximum: maximum, count: count + 1}
    else
      # Skip missed ticks after a stall.
      beat_loop(min(woke + 1_000_000, deadline), deadline, maximum, count + 1)
    end
  end

  defp scan_loop(kind, storage, metrics, deadline, count, within_window, started) do
    before = now()

    if before >= deadline do
      %{count: count, within_window: within_window, started: started, stopped: before}
    else
      scan(kind, storage, metrics)
      within_window = if now() <= deadline, do: within_window + 1, else: within_window
      scan_loop(kind, storage, metrics, deadline, count + 1, within_window, started || before)
    end
  end

  defp scan(:scrape, storage, metrics) do
    _ = NIF.nif_get_all_metrics(storage, metrics)
    :ok
  end

  defp scan(:storage_size, storage, _metrics) do
    _ = NIF.storage_size(storage)
    :ok
  end

  defp scan(:prune, storage, _metrics), do: NIF.prune_tags(storage, [%{id: -1}])

  defp report(phase, scans, results) do
    writer = results.writer
    if writer.count == 0, do: raise("#{phase}: writer made no updates; measurement window missed")

    scan_counts =
      Enum.map_join(scans, ", ", fn kind ->
        scan = Map.fetch!(results, kind)

        if scan.count == 0 or
             min(scan.stopped, writer.stopped) <= max(scan.started, writer.started) do
          raise "#{phase}: #{kind} did not overlap the writer; increase --duration-ms"
        end

        "#{kind}=#{scan.count} (#{scan.within_window} by deadline)"
      end)

    elapsed = writer.stopped - writer.started
    rate = Float.round(writer.count * 1_000_000_000 / elapsed, 1)

    IO.puts(
      "\n#{phase}: updates=#{writer.count}, updates/s=#{rate}, elapsed_ms=#{Float.round(elapsed / 1_000_000, 1)}"
    )

    IO.puts(
      "  insert_us p50=#{percentile(writer, 50)} p99=#{percentile(writer, 99)} max=#{microseconds(writer.maximum)} >1ms=#{writer.slow}"
    )

    IO.puts(
      "  heartbeat_worst_us=#{microseconds(results.heartbeat.maximum)}, wakes=#{results.heartbeat.count}; scans: #{if scans == [], do: "none", else: scan_counts}"
    )
  end

  defp percentile(writer, percent) do
    rank = div(writer.count * percent + 99, 100)

    bin =
      Enum.reduce_while(1..(@histogram_limit + 1), 0, fn bin, seen ->
        seen = seen + :counters.get(writer.histogram, bin)
        if seen >= rank, do: {:halt, bin}, else: {:cont, seen}
      end)

    if bin > @histogram_limit, do: ">#{@histogram_limit}", else: "<=#{bin}"
  end

  defp now, do: System.monotonic_time(:nanosecond)
  defp microseconds(ns), do: Float.round(ns / 1_000, 1)
  defp remaining_ms(deadline), do: max(0, div(deadline - now() + 999_999, 1_000_000))

  defp sleep_until(deadline) do
    receive do
    after
      remaining_ms(deadline) -> :ok
    end
  end
end

PeepContentionBench.run(System.argv())
