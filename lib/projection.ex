defmodule Kvasir.Projection do
  alias Kvasir.Projector.Config
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

    static_opts = opts |> Keyword.take(Config.keys()) |> Keyword.put(:group, name)

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
        {projector, opts} = Keyword.pop(opts, :projection_opts, [])
        config = projector |> Keyword.merge(unquote(static_opts)) |> Keyword.merge(opts)

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
    do: __apply__(projection, event, :no_state, on_error, nil)

  @spec apply(module, Kvasir.Event.t(), state :: term, fun) ::
          :ok | {:ok, state :: term} | :delete | {:error, atom}
  def apply(projection, event, state, on_error),
    do: __apply__(projection, event, {:state, state}, on_error, nil)

  defp do_apply(projection, event, state)
  defp do_apply(projection, event, :no_state), do: projection.apply(event)
  defp do_apply(projection, event, {:state, state}), do: projection.apply(event, state)

  defp __apply__(projection, event, state, on_error, context) do
    result =
      try do
        do_apply(projection, event, state)
      rescue
        err -> err
      end

    case result do
      :ok ->
        :ok

      :delete ->
        :delete

      r = {:ok, _} ->
        r

      err ->
        ctx =
          if is_map(context) do
            %{context | history: [err | context.history], attempts: context.attempts + 1}
          else
            %Kvasir.Projection.Context{
              projection: projection,
              projection_type: if(state == :no_state, do: :global, else: :stateful),
              history: [err],
              attempts: 1,
              event: event
            }
          end

        cond do
          handle_error(err, ctx, on_error) == :ok -> :ok
          projection.back_off(ctx.attempts) != :retry -> err
          :retry -> __apply__(projection, event, state, on_error, ctx.attempts)
        end
    end
  end

  def handle_error(err, _ctx, :error), do: err

  def handle_error(err, ctx, :skip) do
    Logger.warn(fn -> "Projection Skipped: #{inspect(err)}" end, context: ctx)
    :ok
  end

  def handle_error(err, _ctx, callback) when is_function(callback, 1), do: callback.(err)
  def handle_error(err, ctx, callback) when is_function(callback, 2), do: callback.(err, ctx)
end
