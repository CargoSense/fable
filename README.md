# Fable

Write simple, event-driven applications.

[![Package](https://img.shields.io/hexpm/v/fable?logo=elixir&style=for-the-badge)](https://hex.pm/packages/fable)
[![Downloads](https://img.shields.io/hexpm/dt/fable?logo=elixir&style=for-the-badge)](https://hex.pm/packages/fable)
[![Build](https://img.shields.io/github/actions/workflow/status/CargoSense/fable/ci.yml?branch=main&logo=github&style=for-the-badge)](https://github.com/CargoSense/fable/actions/workflows/ci.yml)

## Design Philosophy

- Easy to retrofit.
- Drop-in compatible with Ecto tests (async included).
- Event log should always be consistent.
- Events are serialized around important "aggregate" database records.

## Installation

Add Fable in your project's `mix.exs` file:

```elixir
defp deps do
  [
    {:fable, "~> 0.0.1-alpha.1"}
  ]
end
```

Add the Fable migration to `priv/repo/migrations` and migrate it.

## Usage

As an example, we're going to wrap the following existing function:

```elixir
defmodule MyApp.Blog do
  alias MyApp.Blog.Post

  def create_post(params, user) do
    with :ok <- is_admin(user) do
      %Post{}
      |> Post.changeset(params)
      |> MyApp.Repo.insert()
    end
  end

  def update_post(post, params, user) do
    with :ok <- is_admin(user) do
      post
      |> Post.changeset(params)
      |> MyApp.Repo.update()
    end
  end
end
```

### Create the `MyApp.Events` module

First, create an events module that will handle all the events added to the system.

```elixir
defmodule MyApp.Events do
  use Fable.Events

  @impl Fable.Router
  def handlers do
    %{}
  end
end
```

By default, the module will implement `Fable.Router.handlers/0`. This can be changed by using `use Fable.Events, router: MyApp.EventsRouter`.

### Creating Events

The example system consists of two actions: creating a blog post and updating a blog post. Using the past tense, define `PostCreated` and `PostUpdated` events.

```elixir
defmodule MyApp.Blog.Events.PostCreated do
  use Fable.Event

  embedded_schema do
    field :title, :string
    field :body, :string
  end
end

defmodule MyApp.Blog.Events.PostUpdated do
  use Fable.Event

  embedded_schema do
    field :title, :string
    field :body, :string
  end
end
```

The data of these embedded schemas will be stored in Fable's event table along with some metadata. The schemas will  hold the data your application needs to run the task it deals with. In this example case, all the data passed to the changeset function for posts to be persisted.

### Wrapping Existing Code

With the events being created, wrap the existing code to make use of `:fable`.

#### Moving logic to a separate function

First, move what shall happen as result of the event to a separate function.

```elixir
defmodule MyApp.Blog do
  alias MyApp.Blog.Post
  alias MyApp.Blog.Events

  def create_post(params, user) do
    with :ok <- is_admin(user) do
      # moved
    end
  end

  defp post_created(post, %Events.PostCreated{} = event) do
    post
    |> Post.changeset(Map.from_struct(event))
    |> MyApp.Repo.insert()
  end

  def update_post(post, params, user) do
    with :ok <- is_admin(user) do
      # moved
    end
  end

  defp post_updated(post, %Events.PostUpdated{} = event) do
    post
    |> Post.changeset(Map.from_struct(event))
    |> MyApp.Repo.update()
  end
end
```

#### Registering event handlers

Note that the new functions are private and should never be called externally. To make them be callable by Fable, register handlers for their respective events. One way to do this is by having `MyApp.Blog` implement `Fable.Router` and letting `MyApp.Events` call `handlers/0` on `MyApp.Blog`. This is only example of how to handle event registration.

```elixir
defmodule MyApp.Blog do
  # …

  @behaviour Fable.Router
  @impl Fable.Router
  def handlers do
    %{
      Events.PostCreated => &post_created/2,
      Events.PostUpdated => &post_updated/2
    }
  end

  # …
end

defmodule MyApp.Events do
  use Fable.Events

  @impl Fable.Router
  def handlers do
    %{}
    |> Map.merge(MyApp.Blog.handlers(), &merge/3)
  end

  # If multiple merged maps want to handle the same event
  defp merge(_event, handlers_a, handlers_b) do
    List.wrap(handlers_a) ++ List.wrap(handlers_b)
  end
end
```

#### Emitting events

The last thing to do is emit the events so that the code in the hevent andlers is called.

```elixir
defmodule MyApp.Blog do
  # …

  def create_post(params, user) do
    %Post{id: Ecto.UUID.generate()}
    |> MyApp.Events.emit(fn _, _, _ ->
      with :ok <- is_admin(user) do
        %Events.PostCreated{
          title: params["title"],
          body: params["body"]
        }
      end
    end)
    |> MyApp.Repo.transaction()
  end

  # …

  def update_post(post, params, user) do
    post
    |> MyApp.Events.emit(fn _, _, _ ->
      with :ok <- is_admin(user) do
        %Events.PostUpdated{
          title: params["title"],
          body: params["body"]
        }
      end
    end)
    |> MyApp.Repo.transaction()
  end

  # …
end

defmodule MyApp.Blog.Post do
  use Ecto.Schema

  schema "posts" do
    …

    # needs a migration as well
    field :last_event_id, :integer
  end
end
```

> [!IMPORTANT]
> Note that the aggregate (the post) will need the `:last_event_id` field added on the schema and in the database. The aggregate also needs an `id` before events can be applied to it. This is simple for UUID-based IDs as shown. Using integer-based IDs is supported, but initial creation cannot be handled by a Fable event. Consider using in combination with `Ecto.Multi`.
