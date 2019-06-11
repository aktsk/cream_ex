defmodule Cream.Protocol.Binary.Wire.Get do
  use Cream.Protocol.Binary.Wire

  @opcode 0x00

  @response [
    extras: [
      flags: 4
    ]
  ]
end
