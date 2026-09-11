defmodule Peep.Storage.RustNIF do
  use Rustler, otp_app: :peep_rs, crate: "peep_storage_rustler"

  @behaviour Peep.Storage

  @impl true
  def new(_), do: :erlang.nif_error(:nif_not_loaded)

  # Cache labels at registration; the NIF cannot call Elixir bucket calculators.
  @impl true
  def register_metrics(storage, ids_to_metrics) do
    nif_register_metrics(storage, with_bucket_labels(ids_to_metrics))
  end

  def nif_register_metrics(_storage, _ids_to_metrics), do: :erlang.nif_error(:nif_not_loaded)

  defp with_bucket_labels(ids_to_metrics) do
    ids_to_metrics
    |> Tuple.to_list()
    |> Enum.map(&put_bucket_labels/1)
    |> List.to_tuple()
  end

  # Reuse Peep's stored boundaries. The extra key requires a map pattern,
  # since it is not a declared Distribution field.
  defp put_bucket_labels(
         %{__struct__: Telemetry.Metrics.Distribution, peep_bucket_boundaries: boundaries} =
           metric
       ) do
    # Strip NIF metadata before calling the typed calculator API.
    {mod, config} = Peep.Buckets.config(Map.delete(metric, :peep_bucket_boundaries))
    labels = Enum.with_index(boundaries, fn _boundary, idx -> mod.upper_bound(idx, config) end)

    Map.put(metric, :peep_bucket_labels, labels ++ [:infinity])
  end

  defp put_bucket_labels(metric), do: metric

  @impl true
  def storage_size(_), do: :erlang.nif_error(:nif_not_loaded)

  # A contended event has not written anything yet. Its continuation keeps the
  # original shard, but waits for the lock on a dirty scheduler.
  @impl true
  def insert_metrics(storage, tag_results, batch) do
    case nif_insert_metrics(storage, tag_results, batch) do
      :ok -> :ok
      {:contended, shard_id} -> nif_insert_metrics_dirty(storage, shard_id, tag_results, batch)
    end
  end

  def nif_insert_metrics(_storage, _tag_results, _batch),
    do: :erlang.nif_error(:nif_not_loaded)

  def nif_insert_metrics_dirty(_storage, _shard_id, _tag_results, _batch),
    do: :erlang.nif_error(:nif_not_loaded)

  @impl true
  def get_all_metrics(storage, persistent) do
    nif_get_all_metrics(storage, Peep.Persistent.ids_to_metrics(persistent))
  end

  def nif_get_all_metrics(_storage, _ids_to_metrics), do: :erlang.nif_error(:nif_not_loaded)

  @impl true
  def prune_tags(_, _), do: :erlang.nif_error(:nif_not_loaded)

  # Select the shard inside the NIF: the process can migrate before the call.
  @impl true
  def resolve(storage), do: storage
end
