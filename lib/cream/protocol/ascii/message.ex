defmodule Cream.Protocol.Ascii.Message do
  defstruct [:opcode, :key, :value, :status, :error]
end
