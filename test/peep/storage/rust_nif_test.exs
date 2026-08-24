defmodule Peep.Storage.RustNIFTest.DescendingBuckets do
  @behaviour Peep.Buckets

  @impl true
  def config(_), do: %{}

  @impl true
  def boundaries(_), do: [1000, 100, 10]

  @impl true
  def bucket_for(_, _), do: 0

  @impl true
  def upper_bound(_, _), do: "1000"

  @impl true
  def number_of_buckets(_), do: 3
end

defmodule Peep.Storage.RustNIFTest do
  # Backend-specific behaviour: errors the NIF raises that the ETS backends have
  # no equivalent for, and the tag-value round-trips that back the claim this
  # backend stores the same terms they do.
  use ExUnit.Case, async: true

  import Bitwise

  alias Telemetry.Metrics

  @storage {Peep.Storage.RustNIF, []}

  describe "tag values" do
    setup do
      counter = Metrics.counter("rustler.test.counter", tags: [:tag])
      name = Peep.Test.start_peep!(storage: @storage, metrics: [counter])
      %{counter: counter, name: name}
    end

    # Tags are copied into a process-independent environment rather than decoded
    # into Rust, so the set of storable terms is whatever `enif_make_copy`
    # accepts, and these round-trip byte for byte.
    for {label, tags} <- [
          {"list", %{tag: ["a", "b"]}},
          {"tuple", %{tag: {1, 2}}},
          {"map", %{tag: %{a: 1}}},
          {"nested", %{tag: [%{a: {1, [2]}}, :b]}},
          {"pid", %{tag: :self}},
          {"reference", %{tag: :make_ref}},
          {"bignum", %{tag: 1 <<< 70}},
          {"bitstring", %{tag: <<1::size(3)>>}},
          {"charlist", %{tag: ~c"abc"}},
          {"empty", %{}}
        ] do
      test "a #{label} tag value round-trips", %{counter: counter, name: name} do
        tags = resolve_tags(unquote(Macro.escape(tags)))

        Peep.Test.insert_metric(name, counter, 1, tags)
        Peep.Test.insert_metric(name, counter, 1, tags)

        assert Peep.get_all_metrics(name) |> Map.fetch!(counter) == %{tags => 2}
        assert %{size: 1, memory: memory} = Peep.storage_size(name)
        assert memory > 0
      end
    end

    test "a non-atom tag key round-trips", %{counter: counter, name: name} do
      tags = %{"str" => 1, 2 => :two}

      Peep.Test.insert_metric(name, counter, 1, tags)

      assert Peep.get_all_metrics(name) |> Map.fetch!(counter) == %{tags => 1}
    end

    test "terms that are == but not =:= stay distinct", %{counter: counter, name: name} do
      for tags <- [%{tag: [1]}, %{tag: [1.0]}, %{tag: {1}}, %{tag: {1.0}}] do
        Peep.Test.insert_metric(name, counter, 1, tags)
      end

      assert Peep.get_all_metrics(name) |> Map.fetch!(counter) == %{
               %{tag: [1]} => 1,
               %{tag: [1.0]} => 1,
               %{tag: {1}} => 1,
               %{tag: {1.0}} => 1
             }
    end

    test "maps built in different orders are one series", %{counter: counter, name: name} do
      wide = Map.new(1..40, fn n -> {n, n} end)

      Peep.Test.insert_metric(name, counter, 1, wide)
      Peep.Test.insert_metric(name, counter, 1, Map.new(Enum.reverse(Map.to_list(wide))))

      assert Peep.get_all_metrics(name) |> Map.fetch!(counter) == %{wide => 2}
    end

    test "prune_tags matches an exotic tag value", %{counter: counter, name: name} do
      Peep.Test.insert_metric(name, counter, 1, %{tag: ["a", "b"]})
      Peep.Test.insert_metric(name, counter, 1, %{tag: ["c"]})

      Peep.prune_tags(name, [%{tag: ["a", "b"]}])

      assert Peep.get_all_metrics(name) |> Map.fetch!(counter) == %{%{tag: ["c"]} => 1}
    end

    test "prune_tags rejects a non-map pattern", %{name: name} do
      error = assert_raise ErlangError, fn -> Peep.prune_tags(name, [:not_a_map]) end
      assert {:peep_storage_error, :bad_tags_map, _} = error.original
    end
  end

  test "an integer tag value does not merge into an equal float one" do
    # 16 is the smallest N whose two tags maps share their top 7 hash bits, so
    # hashbrown's control byte does not filter the pair and tags_match runs.
    counter = Metrics.counter("rustler.test.numeric", tags: [:code])
    name = Peep.Test.start_peep!(storage: @storage, metrics: [counter])

    Peep.Test.insert_metric(name, counter, 1, %{code: 16.0})
    Peep.Test.insert_metric(name, counter, 1, %{code: 16})

    series = Peep.get_all_metrics(name) |> Map.fetch!(counter)

    assert series == %{%{code: 16.0} => 1, %{code: 16} => 1}
  end

  test "a float measurement on a Sum raises bad_measurement" do
    sum = Metrics.sum("rustler.test.sum")
    name = Peep.Test.start_peep!(storage: @storage, metrics: [sum])

    error = assert_raise ErlangError, fn -> Peep.Test.insert_metric(name, sum, 1.5, %{}) end
    assert {:peep_storage_error, :bad_measurement, _} = error.original
  end

  # Both `Peep.EventHandler` and `Peep.Test.insert_metric/4` guard on
  # `is_number/1`, so reaching this at all means calling the backend directly.
  test "a non-numeric last_value measurement raises bad_measurement" do
    gauge = Metrics.last_value("rustler.test.gauge")
    name = Peep.Test.start_peep!(storage: @storage, metrics: [gauge])
    {mod, storage} = Peep.Persistent.storage(name)

    error =
      assert_raise ErlangError, fn ->
        mod.insert_metrics(mod.resolve(storage), {%{}}, [{0, gauge, :nope, 0}])
      end

    assert {:peep_storage_error, :bad_measurement, _} = error.original
  end

  test "an integer last_value stays an integer" do
    gauge = Metrics.last_value("rustler.test.gauge")
    name = Peep.Test.start_peep!(storage: @storage, metrics: [gauge])

    Peep.Test.insert_metric(name, gauge, 10, %{})

    assert Peep.get_all_metrics(name) |> Map.fetch!(gauge) == %{%{} => 10}
  end

  test "descending bucket boundaries are rejected at registration" do
    Process.flag(:trap_exit, true)

    dist =
      Metrics.distribution("rustler.test.dist",
        reporter_options: [peep_bucket_calculator: __MODULE__.DescendingBuckets]
      )

    name = :"rustler_test_#{System.unique_integer([:positive])}"

    assert {:error, {reason, _stacktrace}} =
             Peep.start_link(name: name, storage: @storage, metrics: [dist])

    assert {:peep_storage_error, :unsorted_boundaries, _} = reason
  end

  # `Macro.escape/1` cannot carry a live pid or reference into the generated test.
  defp resolve_tags(%{tag: :self}), do: %{tag: self()}
  defp resolve_tags(%{tag: :make_ref}), do: %{tag: make_ref()}
  defp resolve_tags(tags), do: tags
end
