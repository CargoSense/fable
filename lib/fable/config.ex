defmodule Fable.Config do
  # Documented in `Fable.Events`
  @moduledoc false

  @type process_manager_mode :: :sync | :notify

  @type t :: %Fable.Config{
          repo: Ecto.Repo.t(),
          registry: module(),
          router: Fable.Router.t(),
          event_schema: module(),
          process_manager_mode: process_manager_mode(),
          process_manager_schema: module(),
          json_library: module
        }

  defstruct [
    :repo,
    :registry,
    :router,
    event_schema: Fable.Event,
    process_manager_mode: :notify,
    process_manager_schema: Fable.ProcessManager.State,
    json_library: Jason
  ]

  @spec new(module(), Enumerable.t()) :: t
  def new(module, attrs) do
    attrs =
      attrs
      |> Map.new()
      |> Map.put_new(:process_manager_mode, :notify)
      |> Map.put_new(:router, module)
      |> Map.put_new(:registry, Module.concat(Fable, attrs[:repo]))

    unless attrs.process_manager_mode in [:sync, :notify] do
      raise ArgumentError,
            "invalid :process_manager_mode, expected one of :sync, :notify" <>
              ", got: #{inspect(attrs.process_manager_mode)}"
    end

    struct!(__MODULE__, attrs)
  end
end
