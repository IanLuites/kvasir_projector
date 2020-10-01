defmodule Kvasir.Projection.Metrics do
  @spec create(projector :: module, projection :: module) :: :ok
  def create(projector, projection) do
    metrics = Module.concat(projection, Metrics)

    main = ",projector:#{inspect(projector)},projection:#{inspect(projection)}"

    Code.compile_quoted(
      quote do
        defmodule unquote(metrics) do
          def send(result, start, event)

          def send(result, start, %type{__meta__: %{topic: t, partition: p, timestamp: ts}}) do
            stop = :erlang.monotonic_time()
            done = :erlang.system_time(:millisecond)
            ms = :erlang.convert_time_unit(stop - start, :native, :millisecond)

            event = type.__event__(:type)
            success = success(result)
            publish = UTCDateTime.to_unix(ts, :millisecond)
            project = done - publish
            delay = project - ms

            shared = [
              "|ms|#event:",
              event,
              ",topic:",
              t,
              ",partition:",
              to_string(p),
              unquote(main)
            ]

            Kvasir.Projector.Metrics.Sender.send([
              "kvasir.projection.delay.timer:",
              to_string(delay),
              shared
            ])

            Kvasir.Projector.Metrics.Sender.send([
              "kvasir.projection.apply.timer:",
              to_string(ms),
              shared,
              success
            ])

            Kvasir.Projector.Metrics.Sender.send([
              "kvasir.projection.project.timer:",
              to_string(project),
              shared,
              success
            ])

            :ok
          end

          defp success(:ok), do: ",success:true"
          defp success({:ok, _}), do: ",success:true"
          defp success(_), do: ",success:false"
        end
      end,
      "lib/projection/metrics.ex"
    )

    metrics
  end
end
