defmodule Kvasir.Projection.Context do
  @type t :: %__MODULE__{
          projection: module,
          projection_type: :global | :stateful,
          history: [term],
          attempts: pos_integer(),
          event: term
        }

  defstruct [
    :projection,
    :projection_type,
    :history,
    :attempts,
    :event
  ]
end
