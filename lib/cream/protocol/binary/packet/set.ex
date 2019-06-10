defmodule Cream.Protocol.Binary.Packet.Set do
  use Cream.Protocol.Binary.Packet

  @opcode 0x01

  @request [
    extras: [
      flags: 4,
      expiration: 4
    ]
  ]

end
