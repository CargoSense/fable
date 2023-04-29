defmodule Fable.ProcessManager.Workflow do
  # Executes the process manager workflow.
  @moduledoc false
  alias Fable.ProcessManager
  require Logger

  def execute!(pm, event, repo) do
    case execute(pm, event, repo) do
      {:ok, pm} ->
        pm

      {:retry, interval, pm} ->
        :ok = Process.sleep(interval)
        execute!(pm, event, repo)

      :stop ->
        raise RuntimeError, "failed to execute! (todo)"
    end
  end

  def execute(pm, event, repo) do
    %{module: mod, state: state} = pm

    case mod.handle_event(event.data, state) do
      {:ok, data} ->
        pm
        |> ProcessManager.State.progress_to(event.id, data)
        |> repo.update()

      error ->
        Logger.error("""
        Handler #{pm.name} error:
        #{inspect(error)}
        """)

        handle_error(pm, error, event, repo)
    end
  rescue
    e ->
      Logger.error("""
      Handler #{pm.name} raised exception!
      #{inspect(e)}
      #{Exception.format_stacktrace(__STACKTRACE__)}
      """)

      :stop
  end

  defp handle_error(pm, error, event, repo) do
    %{module: mod, state: state} = pm

    case mod.handle_error(event, error, state) do
      {:retry, interval, new_state} ->
        new_pm =
          pm
          |> ProcessManager.State.update_state(new_state)
          |> repo.update!()

        {:retry, interval, new_pm}

      :stop ->
        Logger.error("""
        Handler #{pm.name} stopped!
        Manual intervention required!
        """)

        ProcessManager.disable(repo, pm.name)

        :stop

      other ->
        Logger.error("""
        Handler #{pm.name} failed to handle error!
        Returned: #{inspect(other)}
        Manual intervention required!
        """)

        ProcessManager.disable(repo, pm.name)

        :stop
    end
  end
end
