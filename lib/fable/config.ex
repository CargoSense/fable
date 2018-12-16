defmodule Fable.Config do
  @moduledoc """

  """
  defstruct [
    :repo,
    :registry,
    repo_opts: [],
    event_table: "events",
    event_schema: Fable.Event,
    event_handler_table: "event_handlers",
    event_handler_table: Fable.EventHandler
  ]

  def new(attrs) do
    __MODULE__
    |> struct!(attrs)
    |> Map.update!(:registry, fn
      nil -> Module.concat(Fable, attrs.repo)
      val -> val
    end)
  end
end
