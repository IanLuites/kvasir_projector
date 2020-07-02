defmodule Kvasir.Projection.Global do
  alias Kvasir.Projection

  @callback apply(Kvasir.Event.t()) :: :ok | {:error, atom}

  def start_link(opts \\ []) do
    %{
      topic: topic,
      source: source
    } = opts[:projector].__projector__(:config)

    config =
      opts
      |> Keyword.take(~w(group only)a)
      |> Keyword.put(:state, {opts[:projection], opts[:on_error] || :error})

    source.subscribe(topic, __MODULE__, config)
  end

  def init(_topic, _partition, projection), do: {:ok, projection}

  def event(event, {projection, on_error}) do
    case Projection.apply(projection, event, on_error) do
      :ok -> :ok
      {:ok, _} -> :ok
      err -> err
    end
  end
end
