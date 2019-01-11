defmodule Fable.HandlerInitializer do
  use GenServer

  alias Fable.EventHandler

  defstruct [
    :config,
    :repo,
    :handle_super,
    handlers: %{}
  ]

  def start_link(config) do
    GenServer.start_link(__MODULE__, config, name: Fable.via(config.registry, __MODULE__))
  end

  def init(config) do
    state = %__MODULE__{
      repo: config.repo,
      handle_super: Fable.via(config.registry, HandlerSupervisor),
      config: config
    }

    notifications = Fable.via(config.registry, Notifications)

    _ = Postgrex.Notifications.listen!(notifications, "event-handler-enabled")
    _ = Postgrex.Notifications.listen!(notifications, "event-handler-disabled")
    state = init_handlers(state)
    {:ok, state}
  end

  def handle_info({:notification, _, _, "event-handler-enabled", name}, state) do
    state =
      EventHandler
      |> state.repo.get_by!(name: name)
      |> add_handler(state)

    {:noreply, state}
  end

  def handle_info({:notification, _, _, "event-handler-disabled", name}, state) do
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
    spec = Fable.Handler.child_spec(%{config: state.config, name: handler.name})
    {:ok, pid} = DynamicSupervisor.start_child(state.handle_super, spec)
    ref = Process.monitor(pid)
    put_in(state.handlers[handler.name], {ref, pid})
  end
end
