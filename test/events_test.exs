defmodule Fable.EventsTest do
  use ExUnit.Case, async: true
  alias Fable.Test.Aggregate

  defmodule Events do
    use Fable.Events, repo: Repo

    def handlers, do: %{}
  end

  describe "emit/2" do
    test "returns a one arity callback with an schema as first argument" do
      aggregate = %Aggregate{id: Ecto.UUID.generate()}

      fun =
        Events.emit(aggregate, fn _aggregate, _repo, _changes ->
          %Aggregate.Created{}
        end)

      assert is_function(fun, 1)
    end
  end

  describe "emit/4" do
    test "returns a ecto multi struct" do
      aggregate = %Aggregate{id: Ecto.UUID.generate()}

      multi =
        Ecto.Multi.new()
        |> Events.emit(aggregate, :aggregate, fn _aggregate, _repo, _changes ->
          %Aggregate.Created{}
        end)

      assert %Ecto.Multi{} = multi
      assert [{:aggregate, {:run, _}}] = Ecto.Multi.to_list(multi)
    end
  end
end
