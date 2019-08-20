defmodule Fable.Router do
  @moduledoc """
  The router maps event names to callbacks handling the events.

  In your router module you need to define a handler callback for each event
  you emit. By default `MyApp.Events` will be your router module.

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
        @behaviour Fable.Router

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
  """
  @type t :: module()

  @doc """
  This function returns a map of routings from event names to their handler callbacks.
  """
  @callback handlers() :: %{optional(Fable.Event.name()) => Fable.Events.handler() | [Fable.Events.handler()]}
end
