defmodule Fable.Events do
  @moduledoc """
  `Fable.Events` is the repository for `Fable.Event`s.

  When used it expects a `:repo` key to be set as an option:

      defmodule MyApp.Events do
        use Fable.Events, repo: MyApp.Repo
      end

  ## Options
  * `:registry` - The name of the registry used for the process managers. Defaults to `Fable.` + repo name
  * `:router` - The module responsible for defining the routing of events. Defaults to `__MODULE__`
  to their handlers
  * `:event_schema` - The schema to be used for persisting events. Defaults to `Fable.Event`
  * `:process_manager_schema` - The schema to be used for persisting process managers' state. Defaults to `Fable.ProcessManager.State`
  * `:json_library` - The library used for json encoding. Defaults to `Jason`

  ## Emitting events

  You can emit events as a standalone action or as part of an
  `Ecto.Multi` pipeline. For both ways you need to pass the resulting term
  to `Repo.transaction` for it to be applied.

  Standalone action:

      def create_aggregate(user, args) do
        %MyApp.Aggregate{id: uuid}
        |> MyApp.Events.emit(fn aggregate, repo, _changes ->
          with {:ok, _} <- can(user, :create_aggregate) do
            %MyApp.Aggregate.Created{args: args, user: user}
          end
        end)
        |> Repo.transaction
      end

  As part of `Ecto.Multi`:

      def create_child_on_aggregate(aggregate, user, args) do
        multi =
          Ecto.Multi.new
          |> MyApp.Events.emit(aggregate, :aggregate, fn aggregate, repo, _changes ->
            with {:ok, _} <- can(user, :create_child) do
              %MyApp.Aggregate.ChildCreated{args: args, user: user}
            end
          end)
          |> Ecto.Multi.run(:child, fn repo, %{aggregate: aggregate} ->
            get_latest_child(aggregate, repo)
          end)

        case Repo.transaction(multi) do
          {:ok, %{child: child}} -> {:ok, child}
          {:error, :aggregate, error, _} -> {:error, error}
        end
      end

  ## Handling events

  By default `MyApp.Events` will also be your router module. Look at `Fable.Router`
  for more information on that.


  """
  @type event_or_events :: Fable.Event.t() | [Fable.Event.t()]
  @type emit_callback :: (Ecto.Schema.t(), Ecto.Repo.t(), Ecto.Multi.changes() -> event_or_events)
  @type transation_fun :: (Ecto.Repo.t() -> any())
  @type handler ::
          (Fable.aggregate(), Fable.Event.t() -> {:ok, Fable.aggregate()} | {:error, term})

  @doc """
  Lists all the events of the aggregate in order.
  """
  @callback log(Fable.aggregate()) :: [Fable.Event.t()]

  @doc """
  Delete the given aggregate and replay all its events, building a new aggregate.
  """
  @callback replay(repo :: Ecto.Repo.t(), Fable.aggregate()) ::
              {:ok, Fable.aggregate()} | {:error, term}

  @doc """
  Creates a function, which emits events for an aggregate when passed to `Repo.transaction`.
  """
  @callback emit(Fable.aggregate(), emit_callback, Keyword.t()) :: transation_fun

  @doc """
  Emit events as step within an `Ecto.Multi` pipeline.
  """
  @callback emit(Ecto.Multi.t(), Fable.aggregate(), Ecto.Multi.name(), emit_callback, Keyword.t()) ::
              Ecto.Multi.t()

  defmacro __using__(opts) do
    quote do
      opts = unquote(opts)
      @__fable_config__ Fable.Config.new(__MODULE__, opts)
      @behaviour unquote(__MODULE__)
      if @__fable_config__.router == __MODULE__ do
        @behaviour Fable.Router
      end

      def child_spec(opts) do
        %{
          id: __MODULE__,
          start: {__MODULE__, :start_link, [opts]},
          type: :supervisor
        }
      end

      @doc false
      def __fable_config__(), do: @__fable_config__

      def whereis(name) do
        case Registry.lookup(__fable_config__().registry, {Fable.ProcessManager, name}) do
          [{pid, _}] -> pid
          _ -> nil
        end
      end

      def start_link(_) do
        Supervisor.start_link(Fable, __fable_config__(), [])
      end

      def log(schema) do
        unquote(__MODULE__).log(@__fable_config__, schema)
      end

      def replay(repo, agg) do
        unquote(__MODULE__).replay(repo, @__fable_config__, agg)
      end

      def emit(aggregate, fun, opts \\ []) do
        unquote(__MODULE__).emit(@__fable_config__, aggregate, fun, opts)
      end

      def emit(multi, aggregate, name, fun, opts \\ []) do
        unquote(__MODULE__).emit(@__fable_config__, multi, aggregate, name, fun, opts)
      end

      def unconditionally_emit(aggregate, event_or_events, opts \\ []) do
        unquote(__MODULE__).unconditionally_emit(
          @__fable_config__,
          aggregate,
          event_or_events,
          opts
        )
      end
    end
  end

  @doc false
  @spec replay(Ecto.Repo.t(), Fable.Config.t(), Fable.aggregate()) ::
          {:ok, Fable.aggregate()} | {:error, term}
  def replay(repo, config, agg) do
    config = Map.put(config, :repo, repo)

    repo.transaction(fn ->
      repo.delete!(agg)
      do_replay(config, agg, log(config, agg))
    end)
  end

  @spec do_replay(Fable.Config.t(), Fable.aggregate(), [Fable.Event.t()]) :: Fable.aggregate()
  defp do_replay(%{repo: repo} = config, %aggregate_schema{id: id}, events) do
    import Ecto.Query

    Enum.reduce(events, struct!(aggregate_schema, %{id: id}), fn event, prev_agg ->
      # Locking should not needed because this is a new aggregate,
      # which is not yet committed and therefore not known to others.
      result = handle(event, prev_agg, config)
      # This won't return on errors, so no need to handle the error case
      new_agg = rollback_on_error(repo, result)

      to_be_updated = updateable_rows(prev_agg)
      update_query = where(aggregate_schema, ^to_be_updated)

      case repo.update_all(update_query, set: [last_event_id: event.id]) do
        {1, _} ->
          :ok

        result ->
          raise """
          Update failed: #{inspect(update_query)}
          Got: #{inspect(result)}
          """
      end

      new_agg
    end)
  end

  defp updateable_rows(%{id: id} = prev_agg) do
    import Ecto.Query

    aggregate_id_matches = dynamic([agg], agg.id == ^id)
    empty_last_event_id = dynamic([agg], is_nil(agg.last_event_id))

    case prev_agg.last_event_id do
      nil ->
        dynamic(^aggregate_id_matches and ^empty_last_event_id)

      _ ->
        prev_agg_and_db_last_event_id_match =
          dynamic(
            [agg],
            agg.last_event_id == ^prev_agg.last_event_id
          )

        dynamic(
          ^aggregate_id_matches and (^empty_last_event_id or ^prev_agg_and_db_last_event_id_match)
        )
    end

    # Must those actually be update? Seems like it's already correct
    # event_id_matches_last_event_id = dynamic([m], m.last_event_id == ^event.id)
  end

  @doc false
  def log(config, agg) do
    import Ecto.Query

    config.event_schema
    |> Fable.Event.for_aggregate(agg)
    |> order_by(asc: :id)
    |> config.repo.all()
    |> Enum.map(&Fable.Event.parse_data(config.repo, &1))
  end

  @doc false
  @spec unconditionally_emit(Fable.Config.t(), Fable.aggregate(), event_or_events, Keyword.t()) ::
          transation_fun
  def unconditionally_emit(config, aggregate, event_or_events, opts \\ []) do
    emit(config, aggregate, fn _, _, _ -> event_or_events end, opts)
  end

  @doc false
  @spec emit(
          Fable.Config.t(),
          Fable.aggregate(),
          emit_callback,
          Ecto.Multi.changes(),
          Keyword.t()
        ) ::
          transation_fun
  def emit(config, %{__meta__: %Ecto.Schema.Metadata{}} = aggregate, fun, changes \\ %{}, opts)
      when is_function(fun, 3) do
    check_schema_fields(aggregate)

    fn repo ->
      config = Map.put(config, :repo, repo)
      aggregate = lock(aggregate, repo) || aggregate
      events = fun.(aggregate, repo, changes)
      result_of_applied_events = handle_events(config, aggregate, events, opts)
      rollback_on_error(repo, result_of_applied_events)
    end
  end

  @doc false
  @spec emit(
          Fable.Config.t(),
          Ecto.Multi.t(),
          Fable.aggregate(),
          Ecto.Multi.name(),
          emit_callback,
          Keyword.t()
        ) ::
          Ecto.Multi.t()
  def emit(config, %Ecto.Multi{} = multi, aggregate, name, fun, opts)
      when is_function(fun, 3) do
    Ecto.Multi.run(multi, name, fn repo, changes ->
      emit(config, aggregate, fun, changes, opts)
      |> repo.transaction()
    end)
  end

  @spec handle_events(Fable.Config.t(), Fable.aggregate(), event_or_events, Keyword.t()) ::
          {:ok, Fable.aggregate()} | term
  defp handle_events(config, aggregate, events, opts) do
    reduce_while_ok(aggregate, List.wrap(events), fn aggregate, %_{} = event ->
      do_emit(config, aggregate, event, opts)
    end)
  end

  @spec do_emit(Fable.Config.t(), Fable.aggregate(), Fable.Event.t(), Keyword.t()) ::
          {:ok, Fable.aggregate()} | {:error, term}
  defp do_emit(%{repo: repo} = config, %module{id: id} = aggregate, event, opts) do
    aggregate
    |> generate(event, config, opts)
    |> repo.insert!()
    |> handle(aggregate, config)
    |> case do
      {:ok, %^module{id: ^id} = aggregate} ->
        {:ok, aggregate}

      {:ok, value} ->
        raise """
        You must return the same aggregate which is: #{inspect(struct(module, %{id: id}))}
        You returned: #{inspect(value)}
        """

      {:error, errors} ->
        {:error, errors}
    end
  end

  @spec handle(Fable.Event.t(), Fable.aggregate(), Fable.Config.t()) ::
          {:ok, Fable.aggregate()} | term
  defp handle(event, aggregate, %{repo: repo} = config) do
    aggregate = %{aggregate | last_event_id: event.id}

    event = Fable.Event.parse_data(repo, event)

    case Map.fetch!(config.router.handlers(), Module.safe_concat([event.type])) do
      functions when is_list(functions) ->
        reduce_while_ok(aggregate, functions, fn aggregate, function ->
          function.(aggregate, event.data)
        end)

      function when is_function(function) ->
        function.(aggregate, event.data)
    end
  end

  @spec generate(Fable.aggregate(), Fable.Event.t(), Fable.Config.t(), Keyword.t()) ::
          Ecto.Changeset.t()
  defp generate(%schema{id: id, last_event_id: prev_event_id}, %type{} = event, config, opts) do
    data = Map.from_struct(event)
    table = schema.__schema__(:source)

    attrs = %{
      prev_event_id: prev_event_id,
      aggregate_table: table,
      aggregate_id: id,
      type: to_string(type),
      version: 0,
      data: data,
      meta: Keyword.get(opts, :meta, %{})
    }

    config.event_schema
    |> struct()
    |> Ecto.Changeset.cast(attrs, Map.keys(attrs))
  end

  @spec rollback_on_error(Ecto.Repo.t(), {:ok | :error, term}) :: term | no_return
  defp rollback_on_error(repo, {:error, error}) do
    repo.rollback(error)
  end

  defp rollback_on_error(_, {:ok, value}) do
    value
  end

  @spec check_schema_fields(Fable.aggregate()) :: :ok | no_return
  defp check_schema_fields(%{last_event_id: _, id: _}), do: :ok

  defp check_schema_fields(schema) do
    raise """
    Only tables with a `last_event_id` and `id` columns can be used as aggregates!
    You gave me:
    #{inspect(schema)}
    """
  end

  @spec lock(Fable.aggregate(), Ecto.Repo.t()) :: Fable.aggregate() | nil
  defp lock(%Ecto.Query{} = query, repo) do
    require Ecto.Query

    query
    |> Ecto.Query.lock("FOR UPDATE")
    |> repo.one
  end

  defp lock(%queryable{id: id}, repo) do
    require Ecto.Query

    queryable
    |> Ecto.Queryable.to_query()
    |> Ecto.Query.where(id: ^id)
    |> lock(repo)
  end

  @spec reduce_while_ok(acc, [element], (acc, element -> {:ok, acc} | term)) :: {:ok, acc} | term
        when acc: term, element: term
  defp reduce_while_ok(acc, elements, callback)
       when is_list(elements) and is_function(callback, 2) do
    Enum.reduce_while(elements, {:ok, acc}, fn
      element, {:ok, acc} -> {:cont, callback.(acc, element)}
      _, value -> {:halt, value}
    end)
  end
end
