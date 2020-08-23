defmodule Kvasir.Projection.Global do
  alias Kvasir.Projection

  @callback apply(Kvasir.Event.t()) :: :ok | {:error, atom}

  def start_link(opts \\ []) do
    %{
      topic: topic,
      source: source
    } = opts[:projector].__projector__(:config)

    base_config =
      opts
      |> Keyword.take(~w(group only)a)
      |> Keyword.put(:state, {opts[:projection], opts[:on_error] || :error})

    IO.inspect(opts)

    if opts[:mode] == :batch do
      config = opts |> Keyword.get(:subscription_opts, mode: :batch) |> Keyword.merge(base_config)
      source.subscribe(topic, __MODULE__.Batched, config)
    else
      config = opts |> Keyword.get(:subscription_opts, []) |> Keyword.merge(base_config)
      source.subscribe(topic, __MODULE__, config)
    end
  end

  def init(_topic, _partition, projection), do: {:ok, projection}

  def event(event, {projection, on_error}) do
    case Projection.apply(projection, event, on_error) do
      :ok -> :ok
      {:ok, _} -> :ok
      err -> err
    end
  end

  defmodule Batched do
    require Logger
    @work_pool 30

    def init(_topic, _partition, projection) do
      workers = Enum.map(1..@work_pool, fn _ -> spawn_worker(projection) end)

      {:ok, {projection, workers}}
    end

    defp spawn_worker(projection) do
      {worker, _} = spawn_monitor(__MODULE__, :worker, [self(), projection])

      receive do
        :available -> worker
      end
    end

    def event_batch(events, {projection, workers}) do
      distribute_work(projection, workers, events)
    end

    defp distribute_work(projection, workers, events, busy \\ %{})

    defp distribute_work(projection, [worker | workers], [event | events], busy) do
      ref = make_ref()
      send(worker, {:work, ref, event})
      distribute_work(projection, workers, events, Map.put(busy, ref, {worker, event}))
    end

    defp distribute_work(projection, workers, [], busy) do
      if busy == %{} do
        :ok
      else
        wait_for_result(projection, workers, [], busy)
      end
    end

    defp distribute_work(projection, [], events, busy) do
      wait_for_result(projection, [], events, busy)
    end

    defp wait_for_result(projection, workers, events, busy) do
      receive do
        {:DOWN, _ref, :process, worker, reason} ->
          if worker in workers do
            Logger.error(fn ->
              "Projector<#{inspect(elem(projection, 0))}>: Worker died while idle: #{
                inspect(reason)
              }"
            end)

            w = [spawn_worker(projection) | Enum.reject(workers, &(&1 == worker))]
            distribute_work(projection, w, events, busy)
          else
            ref = Enum.find_value(busy, fn {k, {w, _}} -> if(w == worker, do: k) end)
            {{_worker, event}, b} = Map.pop!(busy, ref)

            Logger.error(
              fn ->
                "Projector<#{inspect(elem(projection, 0))}>: Worker died while handling event: #{
                  inspect(reason)
                }"
              end,
              event: event
            )

            distribute_work(projection, [spawn_worker(projection) | workers], [event | events], b)
          end

        {:done, ref} ->
          {{worker, _event}, b} = Map.pop!(busy, ref)
          distribute_work(projection, [worker | workers], events, b)

        {:done, _ref, err} ->
          err
      end
    end

    def worker(parent, projection) do
      send(parent, :available)
      do_work(parent, projection)
    end

    @work_timeout 60_000

    defp do_work(parent, state = {projection, on_error}) do
      receive do
        {:work, ref, event} ->
          case Projection.apply(projection, event, on_error) do
            :ok -> send(parent, {:done, ref})
            {:ok, _} -> send(parent, {:done, ref})
            err -> send(parent, {:done, ref, err})
          end
      after
        @work_timeout ->
          unless Process.alive?(parent), do: raise("Lost parent, goodbye.")
      end

      do_work(parent, state)
    end
  end
end
