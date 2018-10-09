defmodule Cream.Coder.Marshal do
  @behaviour Cream.Coder

  def encode(value) do
    {1, ExMarshal.encode(value)}
  end

  def decode(flags, value) do
    import Bitwise, only: [&&&: 2]

    if (flags &&& 0b1) != 0 do
      ExMarshal.decode(value)
    else
      value
    end
  end
end
