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
    case projection.apply(event, state) do
      :ok -> {:reply, :ok, s}
      {:ok, new_state} -> {:reply, :ok, {projection, key, new_state}}
      :delete -> {:stop, :normal, :ok, s}
      err -> {:reply, err, s}
    end
  end
end
