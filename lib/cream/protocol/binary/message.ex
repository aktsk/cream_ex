defmodule Cream.Protocol.Binary.Message do

  # @empty_extra_length 0x00               # 1 byte

  @empty_data_type    0x00               # 1 byte
  @empty_vbucket      0x0000             # 2 bytes
  @empty_opaque       0x00000000         # 4 bytes
  @empty_cas          0x0000000000000000 # 8 bytes

  defstruct [
    :opcode,
    :status,
    :opaque,
    :total_body,
    :extra_length,
    :key_length,
    :value_length,
    cas: @empty_cas,
    data_type: @empty_data_type,
    extras: [],
    key: "",
    value: "",
  ]

  @magic_request  0x80
  @magic_response 0x81

  alias Cream.Protocol.Binary.Opcode
  import Cream.Helper

  def new(opcode, fields \\ []) do
    fields = case fields[:value] do
      {value, cas} -> Keyword.merge(fields, value: value, cas: cas)
      _ -> fields
    end
    %{struct!(__MODULE__, fields) | opcode: Opcode.to_atom(opcode)}
  end

  def iolist(opcode, fields \\ []) do
    new(opcode, fields) |> to_iolist
  end

  def to_iolist(message) do
    opcode        = Opcode.to_integer(message.opcode)
    key_length    = byte_size(message.key)
    extras_iolist = extras_iolist(message.extras)
    extras_length = :erlang.iolist_size(extras_iolist)
    value_length  = byte_size(message.value)
    total_body    = key_length + extras_length + value_length

    []
    |> iolist_append(<<@magic_request :: bytes(1)>>)
    |> iolist_append(<<opcode :: bytes(1)>>)
    |> iolist_append(<<key_length :: bytes(2)>>)
    |> iolist_append(<<extras_length :: bytes(1)>>)
    |> iolist_append(<<message.data_type :: bytes(1)>>)
    |> iolist_append(<<@empty_vbucket :: bytes(2)>>)
    |> iolist_append(<<total_body :: bytes(4)>>)
    |> iolist_append(<<@empty_opaque :: bytes(4)>>)
    |> iolist_append(<<message.cas :: bytes(8)>>)
    |> iolist_append(extras_iolist)
    |> iolist_append(message.key)
    |> iolist_append(message.value)
  end

  def extras_iolist(extras) do
    Enum.map extras, fn
      {:flags, flags} -> <<flags :: bytes(4)>>
      {:ttl, expiry} -> <<expiry :: bytes(4)>>
    end
  end

  def from_binary(data) do
    <<
      @magic_response   :: bytes(1),
      opcode            :: bytes(1),
      key_length        :: bytes(2),
      extra_length      :: bytes(1),
      data_type         :: bytes(1),
      status            :: bytes(2),
      total_body        :: bytes(4),
      opaque            :: bytes(4),
      cas               :: bytes(8),
    >> = data

    new(opcode,
      key_length: key_length,
      extra_length: extra_length,
      data_type: data_type,
      status: status,
      total_body: total_body,
      opaque: opaque,
      cas: cas
    )
  end

  def from_binary(message, data) do
    %{
      extra_length: extra_length,
      key_length: key_length,
      total_body: total_body
    } = message

    value_length = total_body - key_length - extra_length

    <<
      extra :: size(extra_length)-binary,
      key   :: size(key_length)-binary,
      value :: size(value_length)-binary
    >> = data

    %{message |
      extras: parse_extra(message.opcode, extra),
      key: key,
      value: value
    }
  end

  defp parse_extra(opcode, data) when opcode in [:get, :getk, :getkq] do
    <<flags :: bytes(4)>> = data
    [flags: flags]
  end

  defp parse_extra(opcode, _data) when opcode in [:set, :add, :replace, :delete] do
    []
  end

end
