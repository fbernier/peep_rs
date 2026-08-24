defmodule Peep.Storage.RustNIF do
  use Rustler, otp_app: :peep_rs, crate: "peep_storage_rustler"

  @behaviour Peep.Storage

  @impl true
  def new(_), do: :erlang.nif_error(:nif_not_loaded)

  @impl true
  def register_metrics(_, _), do: :erlang.nif_error(:nif_not_loaded)

  @impl true
  def storage_size(_), do: :erlang.nif_error(:nif_not_loaded)

  # Amortize the NIF call overhead by batching inserts
  @impl true
  def insert_metrics(_resolved, _tag_results, _batch), do: :erlang.nif_error(:nif_not_loaded)

  @impl true
  def get_all_metrics(storage, persistent) do
    storage
    |> nif_get_all_metrics(Peep.Persistent.ids_to_metrics(persistent))
    |> relabel_distribution_buckets()
  end

  def nif_get_all_metrics(_storage, _ids_to_metrics), do: :erlang.nif_error(:nif_not_loaded)

  # The NIF can't call `upper_bound/2` for any `Peep.Buckets` implementation,
  # so bucket maps come back keyed by integer index rather than by label.
  defp relabel_distribution_buckets(metrics) do
    Map.new(metrics, fn
      {%Telemetry.Metrics.Distribution{} = metric, tagged_series} ->
        {metric, relabel_series(metric, tagged_series)}

      pair ->
        pair
    end)
  end

  defp relabel_series(_metric, tagged_series) when map_size(tagged_series) == 0 do
    tagged_series
  end

  defp relabel_series(metric, tagged_series) do
    {mod, config} = Peep.Buckets.config(metric)

    [{_tags, sample} | _] = Map.to_list(tagged_series)
    labels = Map.new(Map.keys(sample), &{&1, label(&1, mod, config)})

    Map.new(tagged_series, fn {tags, buckets} ->
      relabeled =
        Map.new(buckets, fn {key, count} ->
          {Map.get_lazy(labels, key, fn -> label(key, mod, config) end), count}
        end)

      {tags, relabeled}
    end)
  end

  defp label(idx, mod, config) when is_integer(idx), do: mod.upper_bound(idx, config)
  defp label(other, _mod, _config), do: other

  @impl true
  def prune_tags(_, _), do: :erlang.nif_error(:nif_not_loaded)

  # No erl_nif equivalent of `:erlang.system_info(:scheduler_id)` exists;
  # `enif_thread_type/0` says what kind of scheduler this is, not which one.
  @impl true
  def resolve(storage), do: {storage, :erlang.system_info(:scheduler_id) - 1}
end
