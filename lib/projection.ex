defmodule Kvasir.Projection do
  @default_concurrency 30

  defmacro __using__(opts \\ []) do
    name = Macro.expand(opts[:name], __CALLER__) || inspect(__CALLER__.module)
    back_off = opts[:back_off] || [to: __MODULE__.BackOff, as: :standard]

    type =
      case opts[:state] do
        s when s in [nil, :global] -> __MODULE__.Global
        :partition -> __MODULE__.Partition
        :key -> __MODULE__.Key
        s -> raise "Unknown state: #{inspect(s)}"
      end

    subscribe_opts =
      opts |> Keyword.take(~w(only on_error persist mode)a) |> Keyword.put(:group, name)

    concurrency =
      case opts[:concurrency] do
        c when is_integer(c) and c >= 1 ->
          c

        c when is_binary(c) ->
          p = if c =~ ~r/^[0-9]+$/, do: c, else: System.get_env(c, "30")
          r = String.to_integer(p)
          if r >= 1, do: r, else: @default_concurrency

        _ ->
          @default_concurrency
      end

    quote location: :keep do
      @behaviour unquote(type)
      @projection unquote(type)

      @doc false
      @spec child_spec(Keyword.t()) :: Supervisor.child_spec()
      def child_spec(opts \\ []) do
        config = Keyword.merge(unquote(subscribe_opts), opts)

        %{
          id: __MODULE__,
          type: :supervisor,
          start: {@projection, :start_link, [config]}
        }
      end

      @doc false
      @spec back_off(non_neg_integer) :: :retry | :fail
      defdelegate back_off(attempt), unquote(back_off)

      @doc false
      @spec __projection__(:concurrency | :mode | :stateful) :: term
      def __projection__(:concurrency), do: unquote(concurrency)
      def __projection__(:mode), do: unquote(opts[:mode] || :single)
      def __projection__(:stateful), do: unquote(type != __MODULE__.Global)
    end
  end

  require Logger

  @doc false
  @spec apply(module, Kvasir.Event.t(), fun) :: :ok | {:error, atom}
  def apply(projection, event, on_error),
    do: __apply__(projection, event, on_error, 0)

  @spec apply(module, Kvasir.Event.t(), state :: term, fun) ::
          :ok | {:ok, state :: term} | :delete | {:error, atom}
  def apply(projection, event, state, on_error),
    do: __apply__(projection, event, state, on_error, 0)

  defp __apply__(projection, event, on_error, attempt) do
    with err when err != :ok <- projection.apply(event) do
      Logger.error("Projection Failed<#{inspect(__MODULE__)}>: #{inspect(err)}",
        event: event,
        projection: __MODULE__,
        projection_type: :global,
        error: err,
        attempt: attempt
      )

      cond do
        handle_error(err, on_error) == :ok -> :ok
        projection.back_off(attempt) != :retry -> err
        :retry -> __apply__(projection, event, on_error, attempt + 1)
      end
    end
  end

  defp __apply__(projection, event, state, on_error, attempt) do
    case projection.apply(event, state) do
      :ok ->
        :ok

      :delete ->
        :delete

      r = {:ok, _} ->
        r

      err ->
        Logger.error("Projection Failed<#{inspect(__MODULE__)}>: #{inspect(err)}",
          event: event,
          projection: __MODULE__,
          projection_type: :stateful,
          error: err,
          attempt: attempt
        )

        cond do
          handle_error(err, on_error) == :ok -> :ok
          projection.back_off(attempt) != :retry -> err
          :retry -> __apply__(projection, event, state, on_error, attempt + 1)
        end
    end
  end

  def handle_error(err, :error), do: err

  def handle_error(err, :skip) do
    Logger.warn(fn -> "Projection Skipped: #{inspect(err)}" end)
    :ok
  end

  def handle_error(err, callback) when is_function(callback, 1), do: callback.(err)
end
