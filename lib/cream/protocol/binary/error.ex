defmodule Cream.Protocol.Binary.Error do
  @statuses [
    {0x0000,  nil},
    {0x0001, :not_found},
    {0x0002, :exists},
    {0x0003, :too_large},
    {0x0004, :invalid_args},
    {0x0005, :not_stored}
  ]

  Enum.each @statuses, fn {integer, atom} ->
    def to_atom(unquote(integer)), do: unquote(atom)
    def to_atom(unquote(atom)), do: unquote(atom)

    def to_integer(unquote(atom)), do: unquote(integer)
    def to_integer(unquote(integer)), do: unquote(integer)
  end

  def to_atom(integer) when is_integer(integer) do
    Integer.to_string(integer, 16) |> String.to_atom
  end
end
