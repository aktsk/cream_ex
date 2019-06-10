defmodule Cream.Protocol.Binary.RequestHeader do
  defstruct [
    magic: 0x80,
    op: nil,
    opcode: nil,
    data_type: 0,
    vbucket_id: 0,
    opaque: 0,
    cas: 0
  ]
end
