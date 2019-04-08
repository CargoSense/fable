defmodule Fable.Config do
  @moduledoc """

  """
  defstruct [
    :repo,
    :registry,
    :router,
    repo_opts: [],
    event_schema: Fable.Event,
    process_manager_schema: Fable.ProcessManager.State,
    json_codec: Jason
  ]

  def new(module, attrs) do
    attrs =
      attrs
      |> Map.new()
      |> Map.put_new(:router, module)
      |> Map.put_new(:registry, Module.concat(Fable, attrs[:repo]))

    struct!(__MODULE__, attrs)
  end
end
