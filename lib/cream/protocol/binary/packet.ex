defmodule Cream.Protocol.Binary.Packet do
  defstruct [
    header: nil,
    extras: [],
    key: nil,
    value: nil
  ]

  @packets %{

    get: %{
      opcode: 0x00,
      response_extras: [
        flags: 4
      ]
    },

    set: %{
      opcode: 0x01,
      request_extras: [
        flags: 4,
        expiration: 4
      ]
    }

  }

  alias Cream.Connection
  alias Cream.Protocol.Binary.{RequestHeader, ResponseHeader}

  import Cream.Utils, only: [bytes: 1]

  def new(fields) do
    header = struct(RequestHeader, fields)
    packet = struct(__MODULE__, fields)

    header = case header do
      %{opcode: nil, op: op} when not is_nil(op) -> %{header | opcode: opcode(op)}
      %{opcode: opcode, op: nil} when not is_nil(opcode) -> %{header | op: op(opcode)}
      _ -> raise ArgumentError, "either :op or :opcode need to be set, but not both"
    end

    %{packet | header: header}
  end

  def to_iodata(packet) do
    import Cream.Utils, only: [bytes: 1]

    header = packet.header

    key = packet.key || ""
    value = packet.value || ""
    extras = encode_extras(header.op, packet.extras)

    key_length    = byte_size(key)
    extras_length = IO.iodata_length(extras)
    value_length  = byte_size(value)

    total_body_length = key_length + extras_length + value_length

    [
      <<header.magic       :: bytes(1)>>,
      <<header.opcode      :: bytes(1)>>,
      <<key_length         :: bytes(2)>>,
      <<extras_length      :: bytes(1)>>,
      <<header.data_type   :: bytes(1)>>,
      <<header.vbucket_id  :: bytes(2)>>,
      <<total_body_length  :: bytes(4)>>,
      <<header.opaque      :: bytes(4)>>,
      <<header.cas         :: bytes(8)>>,

      extras,
      key,
      value
    ]
  end

  def send(conn, fields) do
    data = new(fields) |> to_iodata()
    Connection.send(conn, data)
  end

  def recv(conn) do
    with {:ok, data} <- Connection.recv(conn, 24),
      %{total_body_length: size} = header when size > 0 <- parse_header(data),
      {:ok, data} <- Connection.recv(conn, header.total_body_length)
    do
      new(header, data)
    else
      %ResponseHeader{} = header -> {:ok, %__MODULE__{header: header}}
      error -> error
    end
  end

  defp new(header, data) do
    extras_length = header.extras_length
    key_length = header.key_length

    <<
      extras :: binary-size(extras_length),
      key    :: binary-size(key_length),
      value  :: binary
    >> = data

    {:ok, %__MODULE__{
      header: header,
      extras: decode_extras(header.op, extras),
      key: if(key == "", do: nil, else: key),
      value: if(value == "", do: nil, else: value)
    }}
  end

  defp parse_header(data) do
    <<
      0x81              :: bytes(1),
      opcode            :: bytes(1),
      key_length        :: bytes(2),
      extras_length     :: bytes(1),
      data_type         :: bytes(1),
      status            :: bytes(2),
      total_body_length :: bytes(4),
      opaque            :: bytes(4),
      cas               :: bytes(8)
    >> = data

    %ResponseHeader{
      op: op(opcode),
      opcode: opcode,
      key_length: key_length,
      extras_length: extras_length,
      data_type: data_type,
      status: status,
      total_body_length: total_body_length,
      opaque: opaque,
      cas: cas
    }
  end

  defp opcode(op) do
    @packets[op][:opcode]
  end

  defp op(opcode) do
    Enum.find_value(@packets, fn {op, spec} ->
      if spec[:opcode] == opcode do
        op
      else
        false
      end
    end)
  end

  defp encode_extras(op, extras) do
    request_extras = @packets[op][:request_extras] || []

    Enum.reduce request_extras, [], fn {name, bytes}, acc ->
      data = extras[name] || 0
      size = bytes * 8
      [acc, <<data :: size(size)>>]
    end
  end

  defp decode_extras(op, data) do
    request_extras = @packets[op][:response_extras] || []
    decode_extras(request_extras, data, [])
  end

  defp decode_extras(_, "", []), do: []
  defp decode_extras([], "", acc), do: acc
  defp decode_extras([{name, bytes} | request_extras], data, acc) do
    size = bytes * 8
    <<value :: size(size), data :: binary>> = data
    decode_extras(request_extras, data, [{name, value} | acc])
  end

end
