defmodule Kvasir.Projection.Global do
  @callback apply(Kvasir.Event.t()) :: :ok | {:error, atom}

  def start_link(opts \\ []) do
    %{
      topic: topic,
      source: source
    } = opts[:projector].__projector__(:config)

    config =
      opts
      |> Keyword.take(~w(group only)a)
      |> Keyword.put(:state, opts[:projection])

    source.subscribe(topic, __MODULE__, config)
  end

  def init(_topic, _partition, projection), do: {:ok, projection}

  def event(event, projection) do
    with {:ok, _} <- projection.apply(event), do: :ok
  end
end
