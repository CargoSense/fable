defmodule Fable.ProcessManager.Locks do
  use GenServer

  require Logger

  defstruct [
    :config,
    :conn,
    refs: %{},
    names: %{}
  ]

  def start_link(config) do
    GenServer.start_link(__MODULE__, config, name: Fable.via(config.registry, __MODULE__))
  end

  def acquire(config, name) do
    GenServer.call(Fable.via(config.registry, __MODULE__), {:acquire, name})
  end

  def init(config) do
    {:ok, conn} = Postgrex.start_link(db_config(config.repo))

    state = %__MODULE__{
      conn: conn,
      config: config
    }

    notifications = Fable.via(config.registry, Notifications)

    _ = Postgrex.Notifications.listen!(notifications, "process-manager-enabled")
    _ = Postgrex.Notifications.listen!(notifications, "process-manager-disabled")
    init_handlers(state)

    {:ok, state}
  end

  defp init_handlers(state) do
    state.config.process_manager_schema
    |> Fable.Event.active()
    |> state.config.repo.all
    |> Enum.each(&add_handler(&1, state))
  end

  defp add_handler(handler, state) do
    spec = Fable.ProcessManager.child_spec(%{config: state.config, name: handler.name})

    {:ok, _} =
      DynamicSupervisor.start_child(
        Fable.via(state.config.registry, ProcessManagerSupervisor),
        spec
      )
  end

  def handle_call({:acquire, name}, {pid, _}, state) do
    with {:ok, state} <- not_already_locked(state, name, pid),
         %{rows: [[true]]} <-
           lock(state, name) do
      ref = Process.monitor(pid)
      {:reply, :ok, put_in(state.refs[ref], name)}
    else
      _ ->
        {:reply, :error, state}
    end
  end

  defp not_already_locked(%{names: names} = state, name, pid) do
    case Map.fetch(names, name) do
      {:ok, ^pid} ->
        {:ok, state}

      {:ok, _} ->
        :error

      :error ->
        {:ok, %{state | names: Map.put(names, name, pid)}}
    end
  end

  def handle_info({:DOWN, ref, :process, _, _}, state) do
    {name, state} = pop_in(state.refs[ref])

    state =
      if name do
        unlock(state, name)
        Map.update!(state, :names, &Map.delete(&1, name))
      else
        state
      end

    {:noreply, state}
  end

  def handle_info({:notification, _, _, "process-manager-enabled", name}, state) do
    state.config.process_manager_schema
    |> state.config.repo.get_by!(name: name)
    |> add_handler(state)

    {:noreply, state}
  end

  def handle_info({:notification, _, _, "process-manager-disabled", name}, state) do
    case Map.fetch(state.names, name) do
      {:ok, pid} ->
        :ok =
          DynamicSupervisor.terminate_child(
            Fable.via(state.config.registry, ProcessManagerSupervisor),
            pid
          )

      _ ->
        :ok
    end

    {:noreply, state}
  end

  defp lock(%__MODULE__{config: %{registry: namespace} = config, conn: conn}, name) do
    # The hash here is important because postgres advisory locks are a global
    # namespace. If some other part of the application is also trying
    # to take advisory locks based on row ids there's a conflict possible.
    # By using a hash based on the namespace and name we decrease that probability.
    lock = :erlang.phash2({namespace, name})
    schema = config.process_manager_schema.__schema__(:source)

    lock_query = """
    SELECT pg_try_advisory_lock($1)
    FROM #{schema}
    WHERE name = $2 AND active = true
    """

    Logger.debug("""
    Fable Postgrex:
    #{lock_query} #{inspect([lock, name])}
    """)

    Postgrex.query!(conn, lock_query, [lock, name])
  end

  defp unlock(%__MODULE__{config: %{registry: namespace}, conn: conn}, name) do
    lock = :erlang.phash2({namespace, name})

    lock_query = """
    SELECT pg_advisory_unlock($1)
    """

    Logger.debug("""
    Fable Postgrex:
    #{lock_query} #{inspect([lock])}
    """)

    Postgrex.query!(conn, lock_query, [lock])
  end

  defp db_config(repo) do
    repo.config()
    |> Keyword.put(:pool_size, 1)
    # remove the pool so that the sandbox pool
    # doesn't cause confusion
    |> Keyword.delete(:pool)
  end
end
