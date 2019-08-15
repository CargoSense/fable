defmodule Fable.EventsTest do
  use ExUnit.Case, async: true
  import Ecto.Query
  alias Fable.Test.Aggregate

  defmodule Events do
    use Fable.Events, repo: Repo
  end

  describe "emit/2" do
    test "returns a zero arity callback with an query as first argument" do
      query = from a in Aggregate, where: a.id > 0

      fun =
        Events.emit(query, fn aggregate, repo, _changes ->
          %Aggregate.Created{}
        end)

      assert is_function(fun, 0)
    end

    test "returns a zero arity callback with an schema as first argument" do
      aggregate = %Aggregate{id: Ecto.UUID.generate()}

      fun =
        Events.emit(aggregate, fn aggregate, repo, _changes ->
          %Aggregate.Created{}
        end)

      assert is_function(fun, 0)
    end
  end

  describe "emit/4" do
    test "returns a ecto multi struct" do
      aggregate = %Aggregate{id: Ecto.UUID.generate()}

      multi =
        Ecto.Multi.new()
        |> Events.emit(aggregate, :aggregate, fn aggregate, repo, _changes ->
          %Aggregate.Created{}
        end)

      assert %Ecto.Multi{} = multi
      assert [{:aggregate, {:run, _}}] = Ecto.Multi.to_list(multi)
    end
  end
end
