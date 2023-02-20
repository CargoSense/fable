# Fable

Your events have a story to tell

## Design Philosophy

- Easy to retrofit
- Drop in compatible with Ecto tests (async included)
- No footguns. Event log should always be consistent.
- Events are serialized around important "aggregate" database records.


## Gettings started

Include `fable` in your `mix.exs`:

```elixir
defp deps do
  [
    {:fable, "~> 0.0.1-alpha.0", github: "CargoSense/fable", branch: "master"}
  ]
end
```

Add the fable migration to `priv/repo/migrations` and migrate it.

As an example we're going to wrap the following function already existing in
a system:

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

### Create `MyApp.Events` module

First we need to create an events module, which will be dealing with all the events
being added to the system.

```elixir
defmodule MyApp.Events do
  use Fable.Events

  @impl Fable.Router
  def handlers do
    %{}
  end
end
```

By default the module will implement `Fable.Router.handlers/0`, but this can
be moved to a different module by doing `use Fable.Events, router: MyApp.EventsRouter`.

### Creating events

The two actions of the system are creating a blog post and updating a blog post.
In the past tense this means we'll need a `PostCreated` and a `PostUpdated` event.

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

The data of those embedded schemas will be stored in fables event table besides
some metadata. The schemas will need to hold whatever data you need to actually
run the task it deals with. So in our case all the data passed to the changeset
function for posts to be persisted.

### Wrapping existing code

With the events being created we can move to wrapping the existing code to make
use of `:fable`.

There are few things to do: 

#### Move logic to separate function

First move what shall happen as result of the event to a separate function.

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

#### Event handler registration

The new functions are private, because they should never be called by someone else.
To make them be callable by fable though they need to be registered as handlers for
their respective events. One way to do that is having `MyApp.Blog` implement 
`Fable.Router` as well and letting `MyApp.Events` call `handlers/0` on `MyApp.Blog`.
This is just an example on how to handle event registration. Feel free to customize
how registration of event handlers works.

```elixir
defmodule MyApp.Blog do
  …

  @behaviour Fable.Router
  @impl Fable.Router
  def handlers do
    %{
      Events.PostCreated => &post_created/2,
      Events.PostUpdated => &post_updated/2
    }
  end

  …
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

#### Emit events

The last thing to do is emit the events, so the code in the handlers is called
again.

```elixir
defmodule MyApp.Blog do
  …

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

  …

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

  …
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

The important thing to note here is that the aggregate (the post) will need the
`:last_event_id` field being added on the schema and in the db. But also it needs
to have an id, before events can be applied to it. This is simple for uuid based
ids as shown. Using integer based ids is supported, but initial creation cannot be 
handled by a fable event; consider some combination with `Ecto.Multi`.
