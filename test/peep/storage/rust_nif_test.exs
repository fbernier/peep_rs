defmodule Peep.Storage.RustNIFTest.SameLabelBuckets do
  @behaviour Peep.Buckets

  @impl true
  def config(_), do: %{}

  @impl true
  def boundaries(_), do: [10, 100]

  @impl true
  def bucket_for(_, _), do: 0

  # Distinct bounds can collide after label formatting.
  @impl true
  def upper_bound(_, _), do: "same"

  @impl true
  def number_of_buckets(_), do: 2
end

defmodule Peep.Storage.RustNIFTest do
  # NIF-specific validation and term round-trips.
  use ExUnit.Case, async: true

  import Bitwise

  alias Telemetry.Metrics
  alias Peep.Storage.RustNIF

  @storage {Peep.Storage.RustNIF, []}

  describe "tag values" do
    setup do
      counter = Metrics.counter("rustler.test.counter", tags: [:tag])
      name = Peep.Test.start_peep!(storage: @storage, metrics: [counter])
      %{counter: counter, name: name}
    end

    # enif_make_copy preserves arbitrary tag terms.
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
        assert %{size: 1} = Peep.storage_size(name)
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
    counter = Metrics.counter("rustler.test.numeric", tags: [:code])
    name = Peep.Test.start_peep!(storage: @storage, metrics: [counter])

    Peep.Test.insert_metric(name, counter, 1, %{code: 16.0})
    Peep.Test.insert_metric(name, counter, 1, %{code: 16})

    series = Peep.get_all_metrics(name) |> Map.fetch!(counter)

    assert series == %{%{code: 16.0} => 1, %{code: 16} => 1}
  end

  test "rejected samples do not retain new tag sets" do
    sum = Metrics.sum("rustler.test.rejected.sum")
    gauge = Metrics.last_value("rustler.test.rejected.gauge")

    dist =
      Metrics.distribution("rustler.test.rejected.dist")
      |> Map.put(:peep_bucket_boundaries, [10])
      |> Map.put(:peep_bucket_labels, ["10", :infinity])

    storage = RustNIF.new([])
    :ok = RustNIF.nif_register_metrics(storage, {sum, gauge, dist})
    before = RustNIF.storage_size(storage)

    for {id, metric, value, reason} <- [
          {0, sum, 1.5, :bad_measurement},
          {1, gauge, :invalid, :bad_measurement},
          {1, gauge, 1 <<< 70, :bad_measurement},
          {2, dist, 1.0e20, :bad_measurement},
          {3, sum, 1, :unknown_metric_id}
        ] do
      error =
        assert_raise ErlangError, fn ->
          RustNIF.insert_metrics(storage, {%{id: {id, value}}}, [{id, metric, value, 0}])
        end

      assert {:peep_storage_error, ^reason, _} = error.original
    end

    assert RustNIF.nif_get_all_metrics(storage, {sum, gauge, dist}) == %{}
    assert RustNIF.storage_size(storage) == before
  end

  test "a rejected batch item preserves earlier samples without retaining its tags" do
    counter = Metrics.counter("rustler.test.partial.counter")
    sum = Metrics.sum("rustler.test.partial.sum")
    storage = RustNIF.new([])
    control = RustNIF.new([])
    :ok = RustNIF.register_metrics(storage, {counter, sum})
    :ok = RustNIF.register_metrics(control, {counter, sum})
    good = %{id: :accepted}
    bad = %{id: :rejected}

    error =
      assert_raise ErlangError, fn ->
        RustNIF.insert_metrics(storage, {good, bad}, [
          {0, counter, 1, 0},
          {1, sum, 1.5, 1}
        ])
      end

    assert {:peep_storage_error, :bad_measurement, _} = error.original
    :ok = RustNIF.insert_metrics(control, {good}, [{0, counter, 1, 0}])
    assert RustNIF.nif_get_all_metrics(storage, {counter, sum}) == %{counter => %{good => 1}}
    assert RustNIF.storage_size(storage) == RustNIF.storage_size(control)
  end

  test "last_value preserves integer and float measurements exactly" do
    gauge = Metrics.last_value("rustler.test.gauge")
    storage = RustNIF.new([])
    :ok = RustNIF.register_metrics(storage, {gauge})

    :ok =
      RustNIF.insert_metrics(storage, {%{type: :integer}, %{type: :float}}, [
        {0, gauge, 10, 0},
        {0, gauge, 10.5, 1}
      ])

    assert RustNIF.nif_get_all_metrics(storage, {gauge}) ===
             %{gauge => %{%{type: :integer} => 10, %{type: :float} => 10.5}}
  end

  test "bucket boundaries must be strictly increasing" do
    for boundaries <- [[1000, 100, 10], [10, 10, 100]] do
      dist =
        Metrics.distribution("rustler.test.boundaries")
        |> Map.put(:peep_bucket_boundaries, boundaries)
        |> Map.put(:peep_bucket_labels, ["a", "b", "c", :infinity])

      error =
        assert_raise ErlangError, fn ->
          RustNIF.nif_register_metrics(RustNIF.new([]), {dist})
        end

      assert {:peep_storage_error, :unsorted_boundaries, _} = error.original
    end
  end

  test "duplicate labels from a bucket calculator are rejected at registration" do
    dist =
      Metrics.distribution("rustler.test.same_label",
        reporter_options: [peep_bucket_calculator: __MODULE__.SameLabelBuckets]
      )
      |> Map.put(:peep_bucket_boundaries, [10, 100])

    error =
      assert_raise ErlangError, fn ->
        RustNIF.register_metrics(RustNIF.new([]), {dist})
      end

    assert {:peep_storage_error, :bad_argument, _} = error.original
  end

  test "distribution sums and bucket routing retain integer precision past 2^53" do
    boundary = (1 <<< 53) + 1

    dist =
      Metrics.distribution("rustler.test.precision")
      |> Map.put(:peep_bucket_boundaries, [boundary])
      |> Map.put(:peep_bucket_labels, ["wide", :infinity])

    storage = RustNIF.new([])
    :ok = RustNIF.nif_register_metrics(storage, {dist})

    :ok =
      RustNIF.insert_metrics(storage, {%{}}, [
        {0, dist, boundary - 1, 0},
        {0, dist, boundary, 0}
      ])

    assert RustNIF.nif_get_all_metrics(storage, {dist}) ===
             %{dist => %{%{} => %{"wide" => 1, :infinity => 1, :sum => 2 * boundary - 1}}}
  end

  test "a last_value tie compares integer and float values exactly" do
    gauge = Metrics.last_value("rustler.test.tie")
    storage = RustNIF.new([])
    :ok = RustNIF.register_metrics(storage, {gauge})

    bigger = (1 <<< 53) + 1
    smaller = 1.0 * (1 <<< 53)

    :ok =
      RustNIF.insert_metrics(storage, {%{}}, [
        {0, gauge, bigger, 0},
        {0, gauge, smaller, 0}
      ])

    assert RustNIF.nif_get_all_metrics(storage, {gauge}) === %{gauge => %{%{} => bigger}}
  end

  test "signed-zero gauge ties are independent of batch order" do
    gauge = Metrics.last_value("rustler.test.signed_zero")

    for values <- [[0.0, -0.0], [-0.0, 0.0]] do
      storage = RustNIF.new([])
      :ok = RustNIF.register_metrics(storage, {gauge})

      :ok =
        RustNIF.insert_metrics(
          storage,
          {%{}},
          Enum.map(values, &{0, gauge, &1, 0})
        )

      assert RustNIF.nif_get_all_metrics(storage, {gauge}) === %{gauge => %{%{} => 0.0}}
    end
  end

  test "an ids_to_metrics tuple that does not match registration is rejected" do
    counter = Metrics.counter("rustler.test.mismatch")
    sum = Metrics.sum("rustler.test.mismatch.sum")
    storage = RustNIF.new([])
    :ok = RustNIF.register_metrics(storage, {counter, sum})
    :ok = RustNIF.insert_metrics(storage, {%{}}, [{0, counter, 1, 0}])

    for wrong <- [{}, {sum, counter}] do
      error = assert_raise ErlangError, fn -> RustNIF.nif_get_all_metrics(storage, wrong) end
      assert {:peep_storage_error, :metrics_mismatch, _} = error.original
    end

    assert RustNIF.nif_get_all_metrics(storage, {counter, sum}) == %{counter => %{%{} => 1}}
  end

  test "a repeated metric is rejected at registration" do
    counter = Metrics.counter("rustler.test.repeated")
    storage = RustNIF.new([])

    error =
      assert_raise ErlangError, fn ->
        RustNIF.register_metrics(storage, {counter, counter})
      end

    assert {:peep_storage_error, :bad_argument, _} = error.original

    other = Metrics.counter("rustler.test.repeated.other")
    assert :ok = RustNIF.register_metrics(storage, {counter, other})
  end

  test "storage_size follows tag membership changes after a cached read" do
    counter = Metrics.counter("rustler.test.cached_size")
    batch = [{0, counter, 1, 0}]

    fresh = fn tags ->
      storage = RustNIF.new([])
      :ok = RustNIF.register_metrics(storage, {counter})
      Enum.each(tags, &RustNIF.insert_metrics(storage, {&1}, batch))
      storage
    end

    small = %{id: 1}
    large = %{id: 2, data: List.duplicate({:payload, "value"}, 50)}
    storage = fresh.([small])
    assert %{size: 1} = RustNIF.storage_size(storage)

    :ok = RustNIF.insert_metrics(storage, {large}, batch)
    assert RustNIF.storage_size(storage) == RustNIF.storage_size(fresh.([small, large]))

    :ok = RustNIF.prune_tags(storage, [small])
    assert RustNIF.storage_size(storage) == RustNIF.storage_size(fresh.([large]))

    :ok = RustNIF.insert_metrics(storage, {large}, batch)
    assert RustNIF.nif_get_all_metrics(storage, {counter}) == %{counter => %{large => 2}}

    :ok = RustNIF.prune_tags(storage, [%{}])
    assert RustNIF.storage_size(storage) == RustNIF.storage_size(fresh.([]))
  end

  # Pruning compacts tag IDs shared by all metric kinds.
  test "pruning preserves every surviving metric series" do
    counter = Metrics.counter("rustler.test.prune.count", tags: [:id])
    sum = Metrics.sum("rustler.test.prune.bytes", tags: [:id])
    gauge = Metrics.last_value("rustler.test.prune.size", tags: [:id])

    name = Peep.Test.start_peep!(storage: @storage, metrics: [counter, sum, gauge])

    for i <- 1..20 do
      Peep.Test.insert_metric(name, counter, 1, %{id: i})
      Peep.Test.insert_metric(name, sum, i, %{id: i})
      Peep.Test.insert_metric(name, gauge, i, %{id: i})
    end

    :ok = Peep.prune_tags(name, for(i <- 2..20//2, do: %{id: i}))
    survivors = for i <- 1..20//2, do: %{id: i}

    assert Peep.get_all_metrics(name) == %{
             counter => Map.new(survivors, &{&1, 1}),
             sum => Map.new(survivors, &{&1, &1.id}),
             gauge => Map.new(survivors, &{&1, &1.id})
           }
  end

  # `Macro.escape/1` cannot carry a live pid or reference into the generated test.
  defp resolve_tags(%{tag: :self}), do: %{tag: self()}
  defp resolve_tags(%{tag: :make_ref}), do: %{tag: make_ref()}
  defp resolve_tags(tags), do: tags
end
