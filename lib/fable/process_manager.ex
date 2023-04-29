defmodule Fable.ProcessManager do
  use GenServer
  import Ecto.Query
  require Logger

  @callback handle_event(term, map) :: {:ok, map} | term | no_return
  @callback handle_error(term, term, map) :: {:retry, pos_integer, map} | :stop
  @callback start_at(map) :: :head | :origin | pos_integer

  defstruct [
    :config,
    :listen_ref,
    :handler,
    :notifications,
    :repo,
    :name,
    batch_size: 10
  ]

  def child_spec(opts) do
    default = %{
      id: opts.name,
      start: {__MODULE__, :start_link, [opts]}
    }

    Supervisor.child_spec(default, shutdown: 10_000)
  end

  def disable(repo, name) do
    Fable.ProcessManager.State
    |> where(name: ^to_string(name))
    |> repo.update_all(set: [active: false])
  end

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts,
      name: Fable.via(opts.config.registry, {__MODULE__, opts.name})
    )
  end

  def init(%{config: config, name: name}) do
    state = %__MODULE__{
      repo: config.repo,
      config: config,
      name: name,
      notifications: Fable.via(config.registry, Notifications)
    }

    {:ok, state, {:continue, :acquire_lock}}
  end

  def handle_continue(:acquire_lock, state) do
    state
    |> acquire_lock
    |> process_events
    |> case do
      {:ok, state} ->
        {:noreply, state}

      {:error, state} ->
        {:stop, :failed, state}
    end
  end

  # we want to periodically try to get the lock again.
  # if some other process has crashed it might be our turn
  # to get the lock.
  def handle_info(:acquire_lock, %__MODULE__{handler: nil} = state) do
    state
    |> acquire_lock
    |> process_events
    |> case do
      {:ok, state} ->
        {:noreply, state}

      {:error, state} ->
        {:stop, :failed, state}
    end
  end

  def handle_info({:notification, _, ref, "events", data}, %{listen_ref: ref} = state) do
    %__MODULE__{handler: %{last_event_id: last_event_id}} = state

    case Jason.decode!(data) do
      # There's a new event we haven't already pulled it from the database
      %{"id" => event_id} when event_id > last_event_id ->
        state
        |> process_events
        |> case do
          {:ok, state} ->
            {:noreply, state}

          {:error, state} ->
            {:stop, :failed, state}
        end

      _ ->
        {:noreply, state}
    end
  end

  def handle_info(:retry, state) do
    state
    |> process_events
    |> case do
      {:ok, state} ->
        {:noreply, state}

      {:error, state} ->
        {:stop, :failed, state}
    end
  end

  def handle_info({:EXIT, _, :normal}, state) do
    {:noreply, state}
  end

  def handle_info(msg, state) do
    Logger.debug("""
    Unexpected message: #{inspect(msg)} #{inspect(state)}
    """)

    {:noreply, state}
  end

  defp process_events(%__MODULE__{handler: %{active: true}} = state) do
    events = get_events(state)

    events
    |> Enum.reduce_while(state, &handle_event(&1, &2))
    |> case do
      %__MODULE__{handler: nil} = state ->
        {:error, state}

      state ->
        # there may be more, keep going until it looks
        # like we're at the HEAD
        if length(events) == state.batch_size do
          send(self(), :retry)
        end

        {:ok, state}
    end
  end

  defp process_events(state) do
    {:ok, state}
  end

  defp handle_event(event, state) do
    case Fable.ProcessManager.Workflow.execute(state.handler, event, state.repo) do
      {:ok, handler} ->
        {:cont, %__MODULE__{state | handler: handler}}

      {:retry, interval, handler} ->
        Logger.info("Handler #{state.handler.name} retrying in #{interval}...")
        Process.send_after(self(), :retry, interval)
        {:halt, %{state | handler: handler}}

      :stop ->
        {:halt, %{state | handler: nil}}
    end
  end

  defp get_events(state) do
    state.config.event_schema
    |> where([e], e.id > ^state.handler.last_event_id)
    |> order_by(asc: :id)
    |> limit(^state.batch_size)
    |> state.repo.all()
  end

  defp acquire_lock(%__MODULE__{handler: nil} = state) do
    with :ok <- __MODULE__.Locks.acquire(state.config, state.name) do
      Logger.debug("Handler #{state.name} lock acquired on #{inspect(node())}")

      ref = Postgrex.Notifications.listen!(state.notifications, "events")

      %{
        state
        | listen_ref: ref,
          handler: state.repo.get_by!(state.config.process_manager_schema, name: state.name)
      }
    else
      _ ->
        Process.send_after(self(), :acquire_lock, 5_000)
        state
    end
  end

  defp acquire_lock(state) do
    state
  end
end
