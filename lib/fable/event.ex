defmodule Fable.Event do
  import Ecto.Query

  @type t :: Ecto.Schema.t()
  @type name :: module()

  defmacro __using__(_) do
    quote do
      use Ecto.Schema
      @primary_key false
    end
  end

  use Ecto.Schema

  schema "events" do
    field(:prev_event_id, :integer)
    field(:aggregate_id, Ecto.UUID)
    field(:aggregate_table, :string)
    field(:type, :string)
    field(:version, :integer)
    field(:meta, :map, default: %{})
    field(:data, :map, default: %{})
    field(:inserted_at, :utc_datetime, read_after_writes: true)
  end

  def active(queryable) do
    queryable |> where(active: true)
  end

  def for_aggregate(schema \\ __MODULE__, %agg{id: id}) do
    table = agg.__schema__(:source)

    schema
    |> where(aggregate_table: ^table, aggregate_id: ^id)
  end

  def parse_data(repo, event) do
    module = Module.safe_concat([event.type])
    types = %{data: event_ecto_spec(module)}
    %{data: data} = repo.load(types, {[:data], [event.data]})
    %{event | data: data}
  end

  embed_structure =
    if Fable.dependency_vsn_match?(:ecto, "~> 3.5") do
      quote do
        {:parameterized, Ecto.Embedded, var!(struct)}
      end
    else
      quote do
        {:embed, var!(struct)}
      end
    end

  defp event_ecto_spec(module) do
    struct = %Ecto.Embedded{
      cardinality: :one,
      field: :data,
      on_cast: nil,
      on_replace: :raise,
      owner: __MODULE__,
      related: module,
      unique: true
    }

    unquote(embed_structure)
  end
end
