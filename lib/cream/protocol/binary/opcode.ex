defmodule Cream.Protocol.Binary.Opcode do

  @codes %{
    get:        0x00,
    set:        0x01,
    add:        0x02,
    replace:    0x03,
    delete:     0x04,
    increment:  0x05,
    decrement:  0x06,
    quit:       0x07,
    flush:      0x08,
    getq:       0x09,
    noop:       0x0a,
    version:    0x0b,
    getk:       0x0c,
    getkq:      0x0d,
    
    setq:       0x11
  }

  Enum.each @codes, fn {atom, integer} ->
    def to_atom(unquote(atom)), do: unquote(atom)
    def to_atom(unquote(integer)), do: unquote(atom)

    def to_integer(unquote(atom)), do: unquote(integer)
    def to_integer(unquote(integer)), do: unquote(integer)

    def opcode(unquote(atom)), do: unquote(integer)
    def opcode(unquote(integer)), do: unquote(atom)
  end

end
