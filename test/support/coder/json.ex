defmodule Coder.Json do
  @behaviour Cream.Coder

  def encode(value) do
    {1, Poison.encode!(value)}
  end

  def decode(flags, value) do
    import Bitwise, only: [&&&: 2]

    if (flags &&& 0b1) == 0b1 do
      Poison.decode!(value)
    else
      value
    end
  end
  
end
