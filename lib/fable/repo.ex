defmodule Fable.Repo do
  defmacro __using__(_) do
    quote do
      def serial(schema_or_queryable, fun_or_multi, opts \\ []) do
        unquote(__MODULE__).serial(__MODULE__, schema_or_queryable, fun_or_multi, opts)
      end
    end
  end

  require Ecto.Query
  alias Ecto.Multi

  def serial(repo, schema_or_queryable, fun_or_multi, opts \\ [])

  def serial(repo, %Ecto.Query{} = queryable, fun, opts) when is_function(fun, 1) do
    fun = fn ->
      schema = lock(queryable, repo)
      rollback_on_error(repo, fun.(schema))
    end

    repo.transaction(fun, opts)
  end

  def serial(repo, %{__meta__: %Ecto.Schema.Metadata{}} = schema, fun, opts)
      when is_function(fun, 0) do
    fun = fn ->
      lock(schema, repo)
      rollback_on_error(repo, fun.())
    end

    repo.transaction(fun, opts)
  end

  def serial(repo, schema_or_queryable, %Ecto.Multi{} = multi, opts) do
    Multi.new()
    |> Multi.run(:__lock__, fn _, _ ->
      {:ok, lock(schema_or_queryable, repo)}
    end)
    |> Multi.prepend(multi)
    |> repo.transaction(opts)
  end

  defp rollback_on_error(repo, {:error, error}) do
    repo.rollback(error)
  end

  defp rollback_on_error(_, {:ok, value}) do
    value
  end

  def lock(%Ecto.Query{} = query, repo) do
    query
    |> Ecto.Query.lock("FOR UPDATE")
    |> repo.one
  end

  def lock(%queryable{id: id}, repo) do
    queryable
    |> Ecto.Queryable.to_query()
    |> Ecto.Query.where(id: ^id)
    |> lock(repo)
  end
end
