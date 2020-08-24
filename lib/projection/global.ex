defmodule Kvasir.Projection.Global do
  alias Kvasir.Projection

  @callback init(topic :: String.t(), partition :: non_neg_integer) :: :ok | {:error, atom}
  @callback apply(Kvasir.Event.t()) :: :ok | {:error, atom}

  @optional_callbacks init: 2

  def start_link(opts \\ []) do
    %{
      topic: topic,
      source: source
    } = opts[:projector].__projector__(:config)

    base_config =
      opts
      |> Keyword.take(~w(group only)a)
      |> Keyword.put(:state, {opts[:projection], opts[:on_error] || :error})

    if opts[:mode] == :batch do
      config = opts |> Keyword.get(:subscription_opts, mode: :batch) |> Keyword.merge(base_config)
      source.subscribe(topic, __MODULE__.Batched, config)
    else
      config = opts |> Keyword.get(:subscription_opts, []) |> Keyword.merge(base_config)
      source.subscribe(topic, __MODULE__, config)
    end
  end

  def init(topic, partition, projection = {p, _}) do
    if :erlang.function_exported(p, :init, 2) do
      with :ok <- p.init(topic, partition), do: {:ok, projection}
    else
      {:ok, projection}
    end
  end

  def event(event, {projection, on_error}) do
    case Projection.apply(projection, event, on_error) do
      :ok -> :ok
      {:ok, _} -> :ok
      err -> err
    end
  end

  defmodule Batched do
    require Logger

    def init(topic, partition, projection = {p, _}) do
      if :erlang.function_exported(p, :init, 2) do
        with :ok <- p.init(topic, partition) do
          supervisor = spawn_link(__MODULE__, :supervisor, [projection])
          {:ok, supervisor}
        end
      else
        supervisor = spawn_link(__MODULE__, :supervisor, [projection])
        {:ok, supervisor}
      end
    end

    def event_async_batch(ack, events, supervisor) do
      send(supervisor, {:event_set, ack, events})
      :ok
    end

    def supervisor(projection = {p, _}) do
      work_pool = p.__projection__(:concurrency)
      workers = Enum.map(1..work_pool, fn _ -> spawn_worker(projection) end)
      wait_for_events(projection, workers)
    end

    defp wait_for_events(projection, workers) do
      receive do
        {:event_set, ack, events = [e | _]} ->
          acker = {ack, [e.__meta__.offset - 1]}
          distribute_work(acker, projection, workers, events)
      end

      wait_for_events(projection, workers)
    end

    defp do_ack({ack, offsets}, %{__meta__: %{offset: o}}) do
      case ack_calculate(offsets, o) do
        {:no_ack, off} ->
          {ack, off}

        {:ack, v, off} ->
          ack.(v)
          {ack, off}
      end
    end

    defp ack_calculate([h | t], insert) do
      if h == insert - 1 do
        {at, left} = split_trail([insert | t])
        {:ack, at, left}
      else
        {:no_ack, [h | sorted_insert(t, insert)]}
      end
    end

    defp split_trail(list)
    defp split_trail([e]), do: {e, [e]}

    defp split_trail(f = [a, b | t]) do
      if a == b - 1, do: split_trail([b | t]), else: {a, f}
    end

    defp sorted_insert(list, insert, acc \\ [])
    defp sorted_insert([], insert, acc), do: :lists.reverse([insert | acc])

    defp sorted_insert([h | t], insert, acc) do
      if h < insert,
        do: sorted_insert(t, insert, [h | acc]),
        else: :lists.reverse([insert | acc]) ++ t
    end

    defp spawn_worker(projection) do
      {worker, _} = spawn_monitor(__MODULE__, :worker, [self(), projection])

      receive do
        :available -> worker
      end
    end

    defp distribute_work(ack, projection, workers, events, busy \\ %{})

    defp distribute_work(ack, projection, [worker | workers], [event | events], busy) do
      ref = make_ref()
      send(worker, {:work, ref, event})
      distribute_work(ack, projection, workers, events, Map.put(busy, ref, {worker, event}))
    end

    defp distribute_work(ack, projection, workers, [], busy) do
      if busy == %{} do
        :ok
      else
        wait_for_result(ack, projection, workers, [], busy)
      end
    end

    defp distribute_work(ack, projection, [], events, busy) do
      wait_for_result(ack, projection, [], events, busy)
    end

    defp wait_for_result(ack, projection, workers, events, busy) do
      receive do
        {:DOWN, _ref, :process, worker, reason} ->
          if worker in workers do
            Logger.error(fn ->
              "Projector<#{inspect(elem(projection, 0))}>: Worker died while idle: #{
                inspect(reason)
              }"
            end)

            w = [spawn_worker(projection) | Enum.reject(workers, &(&1 == worker))]
            distribute_work(ack, projection, w, events, busy)
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

            distribute_work(
              ack,
              projection,
              [spawn_worker(projection) | workers],
              [event | events],
              b
            )
          end

        {:done, ref} ->
          {{worker, event}, b} = Map.pop!(busy, ref)
          distribute_work(do_ack(ack, event), projection, [worker | workers], events, b)

        {:done, _ref, err} ->
          msg = """
          "Projector<#{inspect(elem(projection, 0))}>: Projection stuck on: #{inspect(err)}
          """

          Logger.error(msg, projection: elem(projection, 0), error: err)

          raise msg
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
