defmodule Kvasir.Projector do
  @moduledoc """
  Documentation for Kvasir.Projector.
  """

  defmodule Stateful do
    @callback apply(Kvasir.Event.t(), state :: any) :: {:ok, any} | {:error, atom}
  end

  defmodule Stateless do
    @callback apply(Kvasir.Event.t()) :: :ok | {:error, atom}
  end

  defmacro __using__(opts \\ []) do
    _behavior =
      if opts[:stateful] do
        quote do
          @behaviour Kvasir.Projector.Stateful
        end
      else
        quote do
          @behaviour Kvasir.Projector.Stateless
        end
      end

    client = opts[:client]
    topic = opts[:topic]
    projections = opts[:projections] || []

    # Disabled environments
    if Mix.env() in (opts[:disable] || []) do
      nil
    else
      Enum.reduce(
        projections,
        nil,
        fn projection, acc ->
          projection = Macro.expand(projection, __CALLER__)

          quote do
            unquote(acc)

            defmodule unquote(Module.concat(projection, Projector)) do
              @moduledoc false
              use Kvasir.Subscriber,
                client: unquote(client),
                topic: unquote(topic)

              @doc false
              @impl Kvasir.Subscriber
              def handle_event(event), do: unquote(projection).apply(event)
            end
          end
        end
      )
    end
  end
end
