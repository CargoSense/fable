defmodule Fable.Events do
  @moduledoc """
  `Fable.Events` is the repository for `Fable.Event`s.

  When used it expects a `:repo` key to be set as an option:

      defmodule MyApp.Events do
        use Fable.Events, repo: MyApp.Repo
      end

  ## Options
  * `:repo` - The ecto repository to be used
  […]

  ## Handlers

  In your events module you need to define a handler callback for each event
  you emit.

      def handlers do
        %{
          MyApp.Aggregate.EventHappened => &MyApp.Aggregate.event_happened/2
        }
      end

  Generally you don't want to have all those callbacks be public api though, which
  is why it's suggested to have those be private within e.g. your context modules
  and only make them available though a single function specifically for the use
  with Fable:

      defmodule MyApp.Context do
        @behaviour Fable.Events

        def handlers do
          %{
            MyApp.Context.Aggregate.EventHappened => &event_happened/2
          }
        end

        defp event_happened(aggregate, event) do
          …
        end
      end

      defmodule MyApp.Events do
        use Fable.Events, repo: MyApp.Repo

        def handlers do
          %{}
          |> Map.merge(MyApp.Context.handlers())
          |> Map.merge(MyApp.AnotherContext.handlers())
        end
      end

  ## Emitting events

  You can emit events as a standalone action or as part of an
  `Ecto.Multi` pipeline. For both ways you need to pass the resulting term
  to `Repo.transaction` for it to be applied.

      def create_aggregate(user, args) do
        %MyApp.Aggregate{id: uuid}
        |> MyApp.Events.emit(fn aggregate, repo, _changes ->
          with {:ok, _} <- can(user, :create_aggregate) do
            %MyApp.Aggregate.Created{args: args, user: user}
          end
        end)
        |> Repo.transaction
      end


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


  """
  @type aggregate_source :: Ecto.Query.t() | Fable.aggregate()
  @type event_or_events :: Fable.Event.t() | [Fable.Event.t()]
  @type emit_callback :: (Ecto.Schema.t(), Ecto.Repo.t(), Ecto.Multi.changes() -> event_or_events)
  @type transation_fun :: (() -> any())
  @type handler ::
          (Fable.aggregate(), Fable.Event.t() -> {:ok, Fable.aggregate()} | {:error, term})

  @callback handlers() :: %{optional(Fable.Event.name()) => handler}

  defmacro __using__(opts) do
    quote do
      opts = unquote(opts)
      @behaviour unquote(__MODULE__)
      @__fable_config__ Fable.Config.new(__MODULE__, opts)

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

      def replay(agg) do
        unquote(__MODULE__).replay(@__fable_config__, agg)
      end

      def emit(schema_or_queryable, fun, opts \\ []) do
        unquote(__MODULE__).emit(@__fable_config__, schema_or_queryable, fun, opts)
      end

      def emit(multi, schema_or_queryable, name, fun, opts \\ []) do
        unquote(__MODULE__).emit(@__fable_config__, multi, schema_or_queryable, name, fun, opts)
      end

      def unconditionally_emit(schema, event_or_events, opts \\ []) do
        unquote(__MODULE__).unconditionally_emit(@__fable_config__, schema, event_or_events, opts)
      end
    end
  end

  @doc false
  def replay(%{repo: repo} = config, agg) do
    repo.transaction(fn ->
      agg |> repo.delete!()
      replay(config, agg, log(config, agg))
    end)
  end

  @doc false
  def replay(%{repo: repo} = config, %module{id: id}, events) do
    import Ecto.Query

    Enum.reduce_while(events, struct!(module, %{id: id}), fn event, prev_agg ->
      repo.serial(prev_agg, fn prev_agg ->
        handle(event, prev_agg, config)
      end)
      |> case do
        {:ok, new_agg} ->
          update_query =
            where(
              module,
              [m],
              m.id == ^id and
                (m.last_event_id == ^prev_agg.last_event_id or m.last_event_id == ^event.id or
                   is_nil(m.last_event_id))
            )

          case repo.update_all(update_query, set: [last_event_id: event.id]) do
            {1, _} ->
              :ok

            result ->
              raise """
              Update failed: #{inspect(update_query)}
              Got: #{inspect(result)}
              """
          end

          {:cont, %{new_agg | last_event_id: event.id}}

        error ->
          {:halt, error}
      end
    end)
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
  @spec emit(Fable.Config.t(), aggregate_source, event_or_events, Keyword.t()) :: transation_fun
  def unconditionally_emit(config, schema_or_queryable, event_or_events, opts \\ []) do
    emit(config, schema_or_queryable, fn _, _, _ -> event_or_events end, opts)
  end

  @doc false
  @spec emit(Fable.Config.t(), aggregate_source, emit_callback, Ecto.Multi.changes(), Keyword.t()) ::
          transation_fun

  def emit(config, schema_or_queryable, fun, changes \\ %{}, opts)

  def emit(%{repo: repo} = config, %Ecto.Query{} = queryable, fun, changes, opts)
      when is_function(fun, 3) do
    fn ->
      schema = lock(queryable, repo) || raise "No aggregate found"
      check_schema_fields(schema)
      rollback_on_error(repo, handle_events(config, schema, fun.(schema, repo, changes), opts))
    end
  end

  def emit(
        %{repo: repo} = config,
        %{__meta__: %Ecto.Schema.Metadata{}} = schema,
        fun,
        changes,
        opts
      )
      when is_function(fun, 3) do
    check_schema_fields(schema)

    fn ->
      schema = lock(schema, repo) || schema
      rollback_on_error(repo, handle_events(config, schema, fun.(schema, repo, changes), opts))
    end
  end

  @doc false
  @spec emit(
          Fable.Config.t(),
          Ecto.Multi.t(),
          aggregate_source,
          term(),
          emit_callback,
          Keyword.t()
        ) ::
          Ecto.Multi.t()
  def emit(config, multi, schema_or_queryable, name, fun, opts)
      when is_function(fun, 3) do
    Ecto.Multi.run(multi, name, fn _repo, changes ->
      emit(config, schema_or_queryable, fun, changes, opts)
    end)
  end

  @spec handle_events(Fable.Config.t(), Fable.aggregate(), event_or_events, Keyword.t()) ::
          {:ok, Fable.aggregate()} | term
  defp handle_events(config, aggregate, %_{} = event, opts),
    do: handle_events(config, aggregate, [event], opts)

  defp handle_events(config, aggregate, events, opts) when is_list(events) do
    reduce_while_ok(aggregate, events, fn aggregate, event ->
      do_emit(config, aggregate, event, opts)
    end)
  end

  @spec do_emit(Fable.Config.t(), Fable.aggregate(), Fable.Event.t(), Keyword.t()) ::
          {:ok, Fable.aggregate()} | {:error, term}
  defp do_emit(%{repo: repo} = config, %module{id: id} = schema, event, opts) do
    schema
    |> generate(event, config, opts)
    |> repo.insert!()
    |> handle(schema, config)
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

  @spec lock(aggregate_source, Ecto.Repo.t()) :: Fable.aggregate() | nil
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
