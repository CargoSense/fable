defmodule Fable.HandlerInitializer do
  use GenServer

  alias Fable.EventHandler

  defstruct [
    :repo,
    :notifications,
    :namespace,
    :handle_super,
    handlers: %{}
  ]

  def start_link(opts) do
    {name, opts} = Keyword.pop(opts, :name)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  def init(opts) do
    state = struct!(__MODULE__, opts)
    _ = Postgrex.Notifications.listen!(state.notifications, "event-handler-inserted")
    _ = Postgrex.Notifications.listen!(state.notifications, "event-handler-deleted")
    state = init_handlers(state)
    {:ok, state}
  end

  def handle_info({:notification, _, _, "event-handler-inserted", name}, state) do
    state =
      EventHandler
      |> state.repo.get_by!(name: name)
      |> add_handler(state)

    {:noreply, state}
  end

  def handle_info({:notification, _, _, "event-handler-deleted", name}, state) do
    state = remove_handler(name, state)
    {:noreply, state}
  end

  defp init_handlers(state) do
    EventHandler
    |> state.repo.all
    |> Enum.reduce(state, &add_handler/2)
  end

  defp remove_handler(handler_name, state) do
    {{ref, pid}, handlers} = Map.pop(state.handlers, handler_name)
    _ = Process.demonitor(ref, flush: true)
    :ok = DynamicSupervisor.terminate_child(state.handler_super, pid)
    %{state | handlers: handlers}
  end

  defp add_handler(handler, state) do
    config = %{
      name: handler.name,
      namespace: state.namespace,
      notifications: state.notifications
    }

    spec = Fable.Handler.child_spec(config)
    {:ok, pid} = DynamicSupervisor.start_child(state.handle_super, spec)
    ref = Process.monitor(pid)
    put_in(state.handlers[handler.name], {ref, pid})
  end
end
