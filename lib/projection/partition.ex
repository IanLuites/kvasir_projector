defmodule Kvasir.Projection.Partition do
  alias Kvasir.Projection
  alias Kvasir.Projection.Metrics

  @callback init(partition :: non_neg_integer) :: {:ok, state :: term}
  @callback apply(Kvasir.Event.t(), state :: term) ::
              :ok | {:ok, state :: term} | :delete | {:error, atom}

  def start_link(opts \\ []) do
    projector = opts[:projector]
    p = opts[:projection]

    %{
      topic: topic,
      source: source
    } = projector.__projector__(:config)

    base_config =
      opts
      |> Keyword.take(~w(group only)a)
      |> Keyword.put(
        :state,
        {p, Metrics.create(projector, p), opts[:on_error] || :error}
      )

    config = opts |> Keyword.get(:subscription_opts, []) |> Keyword.merge(base_config)
    source.subscribe(topic, __MODULE__, config)
  end

  def init(_topic, partition, {projection, metrics, on_error}) do
    with {:ok, state} <- projection.init(partition) do
      {:ok, {projection, metrics, on_error, state}}
    end
  end

  def event(event, {projection, metrics, on_error, state}) do
    start = :erlang.monotonic_time()
    result = Projection.apply(projection, event, state, on_error)
    metrics.send(result, start, event)

    case result do
      :ok -> :ok
      {:ok, new_state} -> {:ok, {projection, on_error, new_state}}
      err -> err
    end
  end
end
