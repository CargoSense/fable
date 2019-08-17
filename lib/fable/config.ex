defmodule Fable.Config do
  # Documented in `Fable.Events`
  @moduledoc false

  @type t :: %Fable.Config{
          repo: Ecto.Repo.t(),
          registry: module(),
          router: Fable.Router.t(),
          repo_opts: Keyword.t(),
          event_schema: module(),
          process_manager_schema: module(),
          json_library: module
        }

  defstruct [
    :repo,
    :registry,
    :router,
    repo_opts: [],
    event_schema: Fable.Event,
    process_manager_schema: Fable.ProcessManager.State,
    json_library: Jason
  ]

  @spec new(module(), Enumerable.t()) :: t
  def new(module, attrs) do
    attrs =
      attrs
      |> Map.new()
      |> Map.put_new(:router, module)
      |> Map.put_new(:registry, Module.concat(Fable, attrs[:repo]))

    struct!(__MODULE__, attrs)
  end
end
