defmodule Coder.Marshal do

  def encode(value) do
    {1, ExMarshal.encode(value)}
  end

  def decode(flags, value) do
    import Bitwise, only: [&&&: 2]

    if (flags &&& 0b1) == 0b1 do
      ExMarshal.decode(value)
    else
      value
    end
  end

end
