defmodule Fable.ModuleColumn do
  use Ecto.Type
  def type, do: :string
  def cast(term), do: {:ok, to_string(term)}
  def load(str), do: {:ok, Module.safe_concat([str])}
  def dump(module), do: {:ok, Atom.to_string(module)}
end
