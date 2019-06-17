defmodule Cream.Protocol.Binary.Opcode do

  alias Cream.Protocol.Binary.Wire

  @opcode_map %{
    0x00 => Wire.Get,
    0x01 => Wire.Set,
    0x0d => Wire.Getkq,
    0x0a => Wire.Noop,
  }

  def get_module(opcode) do
    @opcode_map[opcode]
  end

  def get_specification(opcode) do
    get_module(opcode).specification
  end

end
