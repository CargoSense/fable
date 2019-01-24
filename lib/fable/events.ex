defmodule Fable.Events do
  defmacro __using__(opts) do
    quote do
      opts = unquote(opts)
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

      def start_link(_) do
        Supervisor.start_link(Fable, __fable_config__(), [])
      end

      def log(schema) do
        unquote(__MODULE__).log(@__fable_config__, schema)
      end

      def replay(agg) do
        unquote(__MODULE__).replay(@__fable_config__, agg)
      end

      def emit(schema, event, opts \\ []) do
        unquote(__MODULE__).emit(@__fable_config__, schema, event, opts)
      end

      def emit!(schema, event, opts \\ []) do
        unquote(__MODULE__).emit!(@__fable_config__, schema, event, opts)
      end
    end
  end

  def replay(%{repo: repo} = config, agg) do
    repo.transaction(fn ->
      agg |> repo.delete!()
      replay(config, agg, log(config, agg))
    end)
  end

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

  def log(config, agg) do
    import Ecto.Query

    config.event_schema
    |> Fable.Event.for_aggregate(agg)
    |> order_by(asc: :id)
    |> config.repo.all()
    |> Enum.map(&Fable.Event.parse_data(config.repo, &1))
  end

  def emit!(config, schema, event, opts \\ []) do
    case emit(config, schema, event, opts) do
      {:ok, schema} ->
        schema

      error ->
        raise error
    end
  end

  @spec emit(Fable.Config.t(), Ecto.Queryable.t() | Ecto.Schema.t(), Fable.Event.t(), Keyword.t()) ::
          {:ok, Ecto.Schema.t()} | {:error, term}
  def emit(%{repo: repo} = config, schema_or_queryable, event, opts \\ []) do
    schema =
      case schema_or_queryable do
        %Ecto.Query{} = query -> repo.one!(query)
        schema -> schema
      end

    # We are very intentionally ignoring the retrieved _schema here. If emit/4 has been called,
    # then the user is definitely trying to emit an event based on the version of the aggregate
    # that they know about. It may become out of date, and that should cause an error.
    repo.serial(schema_or_queryable, fn _schema ->
      do_emit(config, schema, event, opts)
    end)
  end

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

  defp handle(event, aggregate, %{repo: repo} = config) do
    aggregate = %{aggregate | last_event_id: event.id}

    event = Fable.Event.parse_data(repo, event)

    case Map.fetch!(config.router.handlers(), Module.safe_concat([event.type])) do
      functions when is_list(functions) ->
        Enum.reduce_while(functions, {:ok, aggregate}, fn
          fun, {:ok, aggregate} ->
            {:cont, fun.(aggregate, event.data)}

          _, value ->
            {:halt, value}
        end)

      function when is_function(function) ->
        function.(aggregate, event.data)
    end
  end

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
end
