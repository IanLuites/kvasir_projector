defmodule Kvasir.Projection do
  defmacro __using__(opts \\ []) do
    name = Macro.expand(opts[:name], __CALLER__) || inspect(__CALLER__.module)

    back_off = opts[:back_off] || (&__MODULE__.BackOff.standard/1)

    type =
      case opts[:state] do
        s when s in [nil, :global] -> __MODULE__.Global
        :partition -> __MODULE__.Partition
        :key -> __MODULE__.Key
        s -> raise "Unknown state: #{inspect(s)}"
      end

    apply =
      case type do
        __MODULE__.Global ->
          quote do
            @doc false
            @spec __apply__(Kvasir.Event.t(), attempt :: non_neg_integer) :: :ok | {:error, atom}
            def __apply__(event, on_error, attempt \\ 0) do
              require Logger

              case apply(event) do
                :ok ->
                  :ok

                err ->
                  require Logger

                  Logger.error("Projection Failed<#{inspect(__MODULE__)}>: #{inspect(err)}",
                    event: event,
                    projection: __MODULE__,
                    projection_type: unquote(opts[:state] || :global),
                    error: err,
                    attempt: 1
                  )

                  cond do
                    unquote(back_off).(attempt) != :retry -> err
                    Kvasir.Projection.handle_error(err, on_error) == :ok -> :ok
                    :retry -> __apply__(event, on_error, attempt + 1)
                  end
              end
            end
          end

        _ ->
          quote do
            @spec __apply__(Kvasir.Event.t(), state :: term, attempt :: non_neg_integer) ::
                    :ok | {:ok, state :: term} | :delete | {:error, atom}
            def __apply__(event, state, on_error, attempt \\ 0) do
              case apply(event, state) do
                :ok ->
                  :ok

                r = {:ok, _} ->
                  r

                err ->
                  require Logger

                  Logger.error("Projection Failed<#{inspect(__MODULE__)}>: #{inspect(err)}",
                    event: event,
                    projection: __MODULE__,
                    projection_type: unquote(opts[:state] || :global),
                    error: err,
                    attempt: 1
                  )

                  cond do
                    Kvasir.Projection.handle_error(err, on_error) == :ok ->
                      :ok

                    unquote(back_off).(attempt) == :retry ->
                      __apply__(event, state, on_error, attempt + 1)

                    :fail ->
                      err
                  end
              end
            end
          end
      end

    subscribe_opts = opts |> Keyword.take(~w(only on_error persist)a) |> Keyword.put(:group, name)

    quote location: :keep do
      @behaviour unquote(type)
      @projection unquote(type)

      @doc false
      @spec child_spec(Keyword.t()) :: Supervisor.child_spec()
      def child_spec(opts \\ []) do
        config = Keyword.merge(unquote(subscribe_opts), opts)

        %{
          id: __MODULE__,
          start: {@projection, :start_link, [config]}
        }
      end

      unquote(apply)
    end
  end

  require Logger

  def handle_error(err, :error), do: err

  def handle_error(err, :skip) do
    Logger.warn(fn -> "Projection Skipped: #{inspect(err)}" end)
    :ok
  end

  def handle_error(err, callback) when is_function(callback, 1), do: callback.(err)
end
