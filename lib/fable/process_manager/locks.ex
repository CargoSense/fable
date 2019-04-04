defmodule Fable.ProcessManager.Locks do
  use GenServer

  alias Fable.ProcessManager

  defstruct [
    :config,
    :conn,
    handlers: %{}
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

    {:ok, state}
  end

  def handle_call({:acquire, name}, {pid, _}, state) do
    case lock(state, name) do
      %{rows: [[true]]} ->
        ref = Process.monitor(pid)
        {:reply, :ok, put_in(state.handlers[ref], name)}

      _ ->
        {:reply, :error, state}
    end
  end

  @lock_query """
  SELECT pg_try_advisory_lock($1)
  FROM event_handlers
  WHERE name = $2 AND active = true
  """
  defp lock(%__MODULE__{config: %{registry: namespace}, conn: conn}, name) do
    # The hash here is important because postgres advisory locks are a global
    # namespace. If some other part of the application is also trying
    # to take advisory locks based on row ids there's a conflict possible.
    # By using a hash based on the namespace and name we decrease that probability.
    lock = :erlang.phash2({namespace, name})
    Postgrex.query!(conn, @lock_query, [lock, name])
  end

  defp db_config(repo) do
    repo.config()
    |> Keyword.put(:pool_size, 1)
    # remove the pool so that the sandbox pool
    # doesn't cause confusion
    |> Keyword.delete(:pool)
  end
end
