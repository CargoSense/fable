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

      def emit(schema, event, opts \\ []) do
        unquote(__MODULE__).emit(@__fable_config__, schema, event, opts)
      end

      def emit!(schema, event, opts \\ []) do
        unquote(__MODULE__).emit!(@__fable_config__, schema, event, opts)
      end
    end
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
    repo.serial(schema_or_queryable, fn schema ->
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
