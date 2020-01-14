defmodule Kvasir.Projector do
  @moduledoc ~S"""
  Documentation for Kvasir.Projector.
  """

  defmacro __using__(opts \\ []) do
    ### Actual Logic ###
    source = opts[:source] || raise "Need to pass the Kvasir EventSource."
    topic = opts[:topic] || raise "Need to pass the Kafka topic."
    projections = Kvasir.Projector.Config.projections!(opts)
    {state, state_opts} = Macro.expand(Kvasir.Projector.Config.state!(opts), __CALLER__)
    {app, version, hex, hexdocs, code_source} = Kvasir.Util.documentation(__CALLER__)

    # Disabled environments
    unless Mix.env() in (opts[:disable] || []) do
      quote do
        require unquote(source)

        @source unquote(source)
        @topic unquote(topic)
        @state unquote({state, Macro.escape(state_opts)})
        @projections unquote(projections)

        @doc false
        @spec child_spec(opts :: Keyword.t()) :: Supervisor.child_sec()
        def child_spec(_opts \\ []), do: unquote(__MODULE__).child_spec(__MODULE__)

        @doc false
        @spec start_link(opts :: Keyword.t()) :: GenServer.on_start()
        def start_link(_opts \\ []), do: unquote(__MODULE__).start_link(__MODULE__)

        @doc false
        @spec __projector__(atom) :: term
        def __projector__(:config),
          do: %{
            projections: @projections,
            projector: __MODULE__,
            source: @source,
            state: @state,
            topic: @topic
          }

        def __projector__(:projections), do: @projections
        def __projector__(:projector), do: __MODULE__
        def __projector__(:source), do: @source
        def __projector__(:state), do: @state
        def __projector__(:topic), do: @topic
        def __projector__(:app), do: {unquote(app), unquote(version)}
        def __projector__(:hex), do: unquote(hex)
        def __projector__(:hexdocs), do: unquote(hexdocs)
        def __projector__(:code_source), do: unquote(code_source)
      end
    end
  end

  def child_spec(projector) do
    %{
      id: projector,
      start: {Kvasir.Projector, :start_link, [projector]}
    }
  end

  def start_link(projector) do
    children =
      :projections
      |> projector.__projector__()
      |> Enum.map(& &1.child_spec(projector: projector, projection: &1))
      |> Enum.reject(&(&1 == :no_start))

    Supervisor.start_link(children, strategy: :one_for_one, name: projector)
  end
end
