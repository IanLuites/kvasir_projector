defmodule Kvasir.Projection.Key do
  @callback init(key :: term) :: {:ok, state :: term}
  @callback apply(Kvasir.Event.t(), state :: term) ::
              :ok | {:ok, state :: term} | :delete | {:error, atom}

  alias Kvasir.Event

  def start_link(opts \\ []) do
    %{
      state: {cache, _},
      topic: topic,
      source: source
    } = opts[:projector].__projector__(:config)

    state =
      if opts[:persist] do
        {opts[:projection], opts[:on_error] || :error, cache}
      else
        {opts[:projection], opts[:on_error] || :error, nil}
      end

    base_config =
      opts
      |> Keyword.take(~w(group only)a)
      |> Keyword.put(:state, state)

    config = opts |> Keyword.get(:subscription_opts, []) |> Keyword.merge(base_config)

    source.subscribe(topic, __MODULE__, config)
  end

  def init(_topic, partition, {projection, on_error, cache}) do
    registry = Module.concat(projection, "Registry#{partition}")
    supervisor = Module.concat(projection, "Supervisor#{partition}")

    with {:ok, _} <- Registry.start_link(keys: :unique, name: registry),
         {:ok, _} <-
           Supervisor.start_link([], name: supervisor, strategy: :one_for_one) do
      {:ok, {registry, supervisor, projection, on_error, cache}}
    end
  end

  def event(event, state = {registry, supervisor, projection, on_error, cache}) do
    key = Event.key(event)

    with {:ok, p} <- projection(registry, supervisor, projection, on_error, key, cache),
         :ok <- project(p, event) do
      :ok
    else
      {:error, :projection_died} -> event(event, state)
      err -> err
    end
  end

  # Work around
  @key_projection_timeout String.to_integer(System.get_env("KEY_PROJECTION_TIMEOUT", "60000"))

  defp project(projection, event) do
    GenServer.call(projection, {:event, event}, @key_projection_timeout)
  rescue
    _ -> {:error, :projection_died}
  end

  ### Projection Management ###

  defp projection(registry, supervisor, projection, on_error, key, cache) do
    if pid = whereis(registry, key) do
      {:ok, pid}
    else
      start(registry, supervisor, projection, on_error, key, cache)
    end
  end

  defp start(registry, supervisor, projection, on_error, key, cache) do
    via_name = {:via, Registry, {registry, key}}

    spec = %{
      id: key,
      start:
        {__MODULE__.Instance, :start_link, [projection, on_error, key, cache, [name: via_name]]},
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
