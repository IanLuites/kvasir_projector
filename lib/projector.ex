defmodule Kvasir.Projector do
  @moduledoc ~S"""
  Documentation for Kvasir.Projector.
  """

  @callback config(atom, Keyword.t()) :: Keyword.t()

  defmacro __using__(opts \\ []) do
    ### Actual Logic ###
    source = opts[:source] || raise "Need to pass the Kvasir EventSource."
    topic = opts[:topic] || raise "Need to pass the Kafka topic."
    {state, state_opts} = Macro.expand(Kvasir.Projector.Config.state!(opts), __CALLER__)
    {app, version, hex, hexdocs, code_source} = Kvasir.Util.documentation(__CALLER__)

    projections =
      opts
      |> Kvasir.Projector.Config.projections!()
      |> Enum.map(&Macro.expand(&1, __CALLER__))
      |> List.flatten()

    if state == false and Enum.any?(projections, & &1.__projection__(:stateful)) do
      raise """
      No state configured.

      State is required for the following projections:

        #{
        projections
        |> Enum.filter(& &1.__projection__(:stateful))
        |> Enum.map(&inspect/1)
        |> Enum.join(", ")
      }
      """
    end

    # Disabled environments
    unless Mix.env() in (opts[:disable] || []) do
      quote location: :keep do
        @behaviour unquote(__MODULE__)
        require unquote(source)

        @source unquote(source)
        @topic unquote(topic)
        @state unquote(state && {state, Macro.escape(state_opts)})
        @projections unquote(projections)

        @doc false
        @spec child_spec(opts :: Keyword.t()) :: Supervisor.child_spec()
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

        @doc false
        @spec config(atom, Keyword.t()) :: Keyword.t()
        @impl unquote(__MODULE__)
        def config(_, opts), do: opts

        defoverridable config: 2
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

    {mod, config} = projector.__projector__(:state)

    caches =
      if mod do
        config = projector.config(:cache, config)
        {:ok, c} = :projections |> projector.__projector__() |> EnumX.map(&mod.init(&1, config))
        c
      else
        []
      end

    Supervisor.start_link(caches ++ children, strategy: :one_for_one, name: projector)
  end
end
