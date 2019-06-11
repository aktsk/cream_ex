defmodule Cream.Protocol.Binary.Wire.Set do
  use Cream.Protocol.Binary.Wire

  @opcode 0x01

  @request [
    extras: [
      flags: 4,
      expiration: 4
    ]
  ]

end
