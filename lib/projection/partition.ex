defmodule Kvasir.Projection.Partition do
  @callback init(partition :: non_neg_integer) :: {:ok, state :: term}
  @callback apply(Kvasir.Event.t(), state :: term) ::
              :ok | {:ok, state :: term} | :delete | {:error, atom}

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

  def init(_topic, partition, projection) do
    with {:ok, state} <- projection.init(partition) do
      {:ok, {projection, state}}
    end
  end

  def event(event, {projection, state}) do
    with {:ok, new_state} <- projection.apply(event, state), do: {:ok, {projection, new_state}}
  end
end
