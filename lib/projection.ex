defmodule Kvasir.Projection do
  defmacro __using__(opts \\ []) do
    name = Macro.expand(opts[:name], __CALLER__) || inspect(__CALLER__.module)

    type =
      case opts[:state] do
        s when s in [nil, :global] -> __MODULE__.Global
        :partition -> __MODULE__.Partition
        :key -> __MODULE__.Key
        s -> raise "Unknown state: #{inspect(s)}"
      end

    subscribe_opts = opts |> Keyword.take(~w(only)a) |> Keyword.put(:group, name)

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
    end
  end
end
