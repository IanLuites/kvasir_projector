defmodule Kvasir.Projection.BackOff do
  @standard %{
    0 => 10,
    1 => 25,
    2 => 50,
    3 => 100,
    4 => 250,
    5 => 500,
    6 => 1000,
    7 => 2500,
    8 => 5000,
    9 => 10_000,
    10 => 30_000,
    11 => 60_000,
    12 => 60_000,
    13 => 60_000,
    14 => 60_000,
    15 => 5 * 60_000,
    16 => 10 * 60_000,
    17 => 15 * 60_000
  }

  def standard(attempt) do
    with {:ok, sleep} <- Map.fetch(@standard, attempt) do
      :timer.sleep(sleep)
      :retry
    end
  end
end