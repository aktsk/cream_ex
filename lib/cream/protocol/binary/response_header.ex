defmodule Cream.Protocol.Binary.ResponseHeader do
  defstruct [
    magic: 0x81,
    op: nil,
    opcode: nil,
    key_length: nil,
    extras_length: nil,
    data_type: nil,
    status: nil,
    total_body_length: nil,
    opaque: nil,
    cas: nil
  ]
end
