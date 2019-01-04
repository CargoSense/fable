defmodule Fable.Event do
  import Ecto.Query

  defmacro __using__(_) do
    quote do
      use Ecto.Schema
      @primary_key false
    end
  end

  def for_aggregate(%agg{id: id}) do
    table = agg.__schema__(:source)

    __MODULE__
    |> where(aggregate_table: ^table, aggregate_id: ^id)
  end

  def parse_data(repo, event) do
    module = Module.safe_concat([event.type])
    types = %{data: event_ecto_spec(module)}
    %{data: data} = repo.load(types, {[:data], [event.data]})
    %{event | data: data}
  end

  defp event_ecto_spec(module) do
    {:embed,
     %Ecto.Embedded{
       cardinality: :one,
       field: :data,
       on_cast: nil,
       on_replace: :raise,
       owner: __MODULE__,
       related: module,
       unique: true
     }}
  end
end
