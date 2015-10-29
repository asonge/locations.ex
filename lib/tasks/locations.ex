defmodule Mix.Tasks.Locations do
  use Mix.Task

  def run(_) do
    Locations.process
  end
end
