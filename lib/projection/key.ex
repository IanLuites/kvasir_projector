defmodule Kvasir.Projection.Key do
  @callback init(key :: term) :: {:ok, state :: term}
  @callback apply(Kvasir.Event.t(), state :: term) ::
              :ok | {:ok, state :: term} | :delete | {:error, atom}

  alias Kvasir.Event

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
    registry = Module.concat(projection, "Registry#{partition}")
    supervisor = Module.concat(projection, "Supervisor#{partition}")

    with {:ok, _} <- Registry.start_link(keys: :unique, name: registry),
         {:ok, _} <-
           Supervisor.start_link([], name: supervisor, strategy: :one_for_one) do
      {:ok, {registry, supervisor, projection}}
    end
  end

  def event(event, state = {registry, supervisor, projection}) do
    key = Event.key(event)

    with {:ok, p} <- projection(registry, supervisor, projection, key),
         :ok <- project(p, event) do
      :ok
    else
      {:error, :projection_died} -> event(event, state)
    end
  end

  defp project(projection, event) do
    GenServer.call(projection, {:event, event}, 60_000)
  rescue
    _ -> {:error, :projection_died}
  end

  ### Projection Management ###

  defp projection(registry, supervisor, projection, key) do
    if pid = whereis(registry, key) do
      {:ok, pid}
    else
      start(registry, supervisor, projection, key)
    end
  end

  defp start(registry, supervisor, projection, key) do
    via_name = {:via, Registry, {registry, key}}

    spec = %{
      id: key,
      start: {__MODULE__.Instance, :start_link, [projection, key, [name: via_name]]},
      restart: :transient
    }

    case Supervisor.start_child(supervisor, spec) do
      {:error, {:already_started, pid}} -> {:ok, pid}
      {:error, :already_present} -> Supervisor.restart_child(supervisor, key)
      reply -> reply
    end
  end

  defp whereis(registry, key) do
    case Registry.lookup(registry, key) do
      [{pid, _}] -> pid
      _ -> nil
    end
  end
end
