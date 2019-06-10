defmodule Cream.Coder.Raw do
  @behaviour Cream.Coder

  def encode(value) do
    {0, value}
  end

  def decode(_flags, value) do
    value
  end
end
