defmodule Cream.Protocol.Binary.Packet.Get do
  use Cream.Protocol.Binary.Packet

  @opcode 0x00

  @response [
    extras: [
      flags: 4
    ]
  ]

end
