defmodule Kvasir.Projection.Key.Instance do
  def start_link(projection, key, cache, opts \\ []) do
    GenServer.start_link(__MODULE__, {projection, key, cache}, opts)
  end

  def init({projection, key, nil}) do
    with {:ok, state} <- projection.init(key) do
      {:ok, {projection, key, state, -1, nil}}
    end
  end

  def init({projection, key, cache}) do
    {init, offset} =
      case cache.load(projection, key) do
        {:ok, o, data} -> {{:ok, data}, o}
        {:error, :not_found} -> {projection.init(key), -1}
        err -> {err, -3}
      end

    with {:ok, state} <- init do
      {:ok, {projection, key, state, offset, cache}}
    end
  end

  def handle_call({:event, event}, _from, s = {projection, key, state, offset, cache}) do
    o = event.__meta__.offset

    if o <= offset do
      {:reply, :ok, s}
    else
      with {:ok, new_state} <- projection.apply(event, state),
           :ok <- store_state(cache, projection, key, o, new_state) do
        {:reply, :ok, {projection, key, new_state, o, cache}}
      else
        :ok -> {:reply, :ok, s}
        :delete -> {:stop, :normal, delete_state(cache, projection, key), s}
        err -> {:reply, err, s}
      end
    end
  end

  defp store_state(cache, projection, key, offset, state)
  defp store_state(nil, _, _, _, _), do: :ok

  defp store_state(cache, projection, key, offset, state),
    do: cache.save(projection, key, state, offset)

  defp delete_state(cache, projection, key)
  defp delete_state(nil, _, _), do: :ok
  defp delete_state(cache, projection, key), do: cache.delete(projection, key)
end
