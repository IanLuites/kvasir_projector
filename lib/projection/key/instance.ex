defmodule Kvasir.Projection.Key.Instance do
  def start_link(projection, key, opts \\ []) do
    GenServer.start_link(__MODULE__, {projection, key}, opts)
  end

  def init({projection, key}) do
    with {:ok, state} <- projection.init(key) do
      {:ok, {projection, key, state}}
    end
  end

  def handle_call({:event, event}, _from, s = {projection, key, state}) do
    with {:ok, new_state} <- projection.apply(event, state),
         :ok <- store_state(projection, key, new_state) do
      {:reply, :ok, {projection, key, new_state}}
    else
      :ok -> {:reply, :ok, s}
      :delete -> {:stop, :normal, delete_state(projection, key), s}
      err -> {:reply, err, s}
    end
  end

  defp store_state(_, _, _), do: :ok
  defp delete_state(_, _), do: :ok
end
