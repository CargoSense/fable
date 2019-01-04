defmodule Fable.EventHandler do
  use Ecto.Schema
  import Ecto.Query
  import Ecto.Changeset

  alias Fable

  schema "event_handlers" do
    field(:last_event_id, :integer, read_after_writes: true)
    field(:name, :string, null: false)
    field(:module, Fable.ModuleColumn, null: false)
    field(:state, :map, default: %{})
    field(:active, :boolean, read_after_writes: true)
    timestamps(type: :utc_datetime_usec)
  end

  def update_state(handler, state) do
    handler
    |> change(%{state: state})
  end

  def progress_to(handler, last_event_id, state) do
    handler
    |> update_state(state)
    |> optimistic_lock(:last_event_id, fn _ -> last_event_id end)
  end
end
