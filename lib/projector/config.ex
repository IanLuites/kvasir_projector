defmodule Kvasir.Projector.Config do
  def state(opts) do
    case opts[:cache] || settings()[:cache] do
      nil -> {false, []}
      {mod, opt} -> {mod, opt}
      mod -> {mod, []}
    end
  end

  def state!(opts), do: state(opts) || raise("state not set for projector or in config.")

  defp settings, do: Application.get_env(:kvasir, :projector, [])

  def projections(opts) do
    case opts[:projections] || opts[:projection] do
      nil -> []
      projections when is_list(projections) -> projections
      projection -> [projection]
    end
  end

  def projections!(opts),
    do: projections(opts) || raise("projections not set for projector or in config.")
end
