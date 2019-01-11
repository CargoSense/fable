defmodule Fable.Handler do
  use GenServer
  import Ecto.Query
  require Logger

  @callback handle_event(term, map) :: {:ok, map} | term | no_return
  @callback handle_error(term, term, map) :: {:retry, pos_integer, map} | :stop
  @callback start_at(map) :: :head | :origin | pos_integer

  defstruct [
    :config,
    :listen_ref,
    :conn,
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
    Fable.EventHandler
    |> where(name: ^to_string(name))
    |> repo.update_all(set: [active: false])
  end

  def start_link(config) do
    GenServer.start_link(__MODULE__, config, [])
  end

  def init(%{config: config, name: name}) do
    Process.flag(:trap_exit, true)
    {:ok, conn} = Postgrex.start_link(db_config(config.repo))

    state = %__MODULE__{
      conn: conn,
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

  def handle_info({:EXIT, pid, _reason}, %__MODULE__{conn: pid} = state) do
    {:stop, :connection_died, state}
  end

  def handle_info(msg, state) do
    Logger.error("""
    Unexpected message: #{inspect(msg)}
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
          process_events(state)
        else
          {:ok, state}
        end
    end
  end

  defp process_events(state) do
    {:ok, state}
  end

  defp handle_event(event, state) do
    case run_handler(state, event) do
      {:ok, data} ->
        state.handler
        |> Fable.EventHandler.progress_to(event.id, data)
        |> state.repo.update()
        |> case do
          {:ok, handler} ->
            {:cont, %__MODULE__{state | handler: handler}}

          {:error, error} ->
            Logger.error("""
            Handler #{state.handler.name} handler error:
            #{inspect(error)}
            Stopping!
            """)

            disable(state.repo, state.handler.name)
            {:halt, %{state | handler: nil}}
        end

      error ->
        Logger.error("""
        Handler #{state.handler.name} error:
        #{inspect(error)}
        """)

        handler =
          case apply(state.handler.module, :handle_error, [event, error, state.handler.state]) do
            {:retry, interval, handler_state} ->
              Logger.info("Handler #{state.handler.name} retrying in #{interval}...")
              Process.send_after(self(), :retry, interval)

              state.handler
              |> Fable.EventHandler.update_state(handler_state)
              |> state.repo.update!()

            :stop ->
              Logger.error("""
              Handler #{state.handler.name} stopped!
              Manual intervention required!
              """)

              disable(state.repo, state.handler.name)

              nil

            other ->
              Logger.error("""
              Handler #{state.handler.name} failed to handle error!
              Returned: #{inspect(other)}
              Manual intervention required!
              """)

              disable(state.repo, state.handler.name)

              nil
          end

        {:halt, %{state | handler: handler}}
    end
  end

  defp run_handler(state, event) do
    apply(state.handler.module, :handle_event, [event, state.handler.state])
  rescue
    e ->
      Logger.error("""
      Handler #{state.handler.name} raised exception!
      #{inspect(e)}
      #{Exception.format_stacktrace(__STACKTRACE__)}
      Manual intervention required!
      """)

      {:error, e}
  end

  defp get_events(state) do
    state.config.event_schema
    |> where([e], e.id > ^state.handler.last_event_id)
    |> order_by(asc: :id)
    |> limit(^state.batch_size)
    |> state.repo.all()
  end

  defp acquire_lock(%__MODULE__{handler: nil} = state) do
    with %{rows: [[true]]} <- do_lock(state) do
      Logger.debug("Handler #{state.name} lock acquired on #{inspect(node())}")

      ref = Postgrex.Notifications.listen!(state.notifications, "events")

      %{
        state
        | listen_ref: ref,
          handler: state.repo.get_by!(Fable.EventHandler, name: state.name)
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

  @lock_query """
  SELECT pg_try_advisory_lock($1)
  FROM event_handlers
  WHERE name = $2 AND active = true
  """
  defp do_lock(%__MODULE__{config: %{registry: namespace}, name: name, conn: conn}) do
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
