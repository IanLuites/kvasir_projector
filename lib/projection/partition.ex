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
      |> Keyword.put(:state, {opts[:projection], opts[:on_error] || :error})

    source.subscribe(topic, __MODULE__, config)
  end

  def init(_topic, partition, {projection, on_error}) do
    with {:ok, state} <- projection.init(partition) do
      {:ok, {projection, on_error, state}}
    end
  end

  def event(event, {projection, on_error, state}) do
    case projection.__apply__(event, state, on_error) do
      :ok -> :ok
      {:ok, new_state} -> {:ok, {projection, on_error, new_state}}
      err -> err
    end
  end
end
