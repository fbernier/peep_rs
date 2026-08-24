defmodule Peep.Storage.RustNIFTest do
  use ExUnit.Case
  doctest Peep.Storage.RustNIF

  test "greets the world" do
    assert Peep.Storage.RustNIF.hello() == :world
  end
end
