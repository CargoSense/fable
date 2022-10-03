defmodule Fable.ProcessManager.State do
  use Ecto.Schema
  import Ecto.Changeset

  alias Fable

  schema "process_managers" do
    field(:last_event_id, :integer, read_after_writes: true)
    field(:name, :string)
    field(:module, Fable.ModuleColumn)
    field(:state, :map, default: %{})
    field(:active, :boolean, read_after_writes: true)
    timestamps(type: :utc_datetime_usec)
  end

  def update_state(handler, state) do
    if handler.state == state do
      handler |> change(%{})
    else
      handler |> change(%{state: state})
    end
  end

  def progress_to(handler, last_event_id, state) do
    handler
    |> update_state(state)
    |> optimistic_lock(:last_event_id, fn _ -> last_event_id end)
    |> change(last_event_id: last_event_id)
  end
end
