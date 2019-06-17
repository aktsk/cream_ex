defmodule Cream.Protocol.Binary.Wire.Getkq do
  use Cream.Protocol.Binary.Wire

  @opcode 0x0d

  @response [
    extras: [
      flags: 4
    ]
  ]
end
