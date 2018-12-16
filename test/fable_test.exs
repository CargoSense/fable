defmodule FableTest do
  use ExUnit.Case
  doctest Fable

  test "greets the world" do
    assert Fable.hello() == :world
  end
end
