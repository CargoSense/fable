defmodule Fable do
  use Supervisor

  @type aggregate :: Ecto.Schema.t()

  @moduledoc """
  Handles the mechanics of event sourcing.

  ## Wishlist

  In the future I want to drive the handler children exclusively off of the
  `stream_cursors` database table.
  """

  def create_handler(events, name, module, initial_state) do
    config = events.__fable_config__

    last_event_id =
      case module.start_at(initial_state) do
        :origin ->
          -1

        :head ->
          config.repo.aggregate(config.event_schema, :max, :id)

        n when is_integer(n) ->
          n
      end

    config.process_manager_schema
    |> struct
    |> Ecto.Changeset.change(%{
      last_event_id: last_event_id,
      name: name,
      module: module,
      state: initial_state
    })
    |> Ecto.Changeset.unique_constraint(:name)
    |> config.repo.insert()
  end

  @doc false
  def via(registry, name) do
    {:via, Registry, {registry, name}}
  end

  @doc false
  def init(%{registry: registry} = config) do
    notifications_name = via(registry, Notifications)

    children = [
      {Registry, keys: :unique, name: registry},
      notifications_child(config.repo, notifications_name),
      {DynamicSupervisor, strategy: :one_for_one, name: via(registry, ProcessManagerSupervisor)},
      {__MODULE__.ProcessManager.Locks, config}
    ]

    Supervisor.init(children, strategy: :rest_for_one)
  end

  defp notifications_child(repo, name) do
    config =
      repo.config()
      |> Keyword.put(:name, name)
      |> Keyword.put(:pool_size, 1)

    %{
      id: Postgrex.Notifications,
      start: {Postgrex.Notifications, :start_link, [config]}
    }
  end
end
