defmodule Cream.Protocol.Binary.Packet do

  defstruct [:info, :header, :body]

  @request_magic 0x80
  @response_magic 0x81

  @default_request_header %{
    magic: @request_magic,
    data_type: 0,
    vbucket_id: 0,
    opaque: 0,
    cas: 0
  }

  @header_keys [:data_type, :vbucket_id, :opaque, :cas]

  @default_request_body %{
    key: nil,
    value: nil,
    extras: []
  }

  @body_keys [:key, :value, :extras]

  @info [
    # {opcode, name, request_extras, response_extras}
    {0x00, :get,    [],                         [flags: 4]},
    {0x01, :set,    [flags: 4, expiration: 4],  []},
    {0x0a, :noop,   [],                         []},
    {0x0d, :getkq,  [],                         [flags: 4]},
  ]

  @info_by_opcode Enum.reduce(@info, %{}, fn spec, acc ->
    {opcode, name, request_extras, response_extras} = spec
    Map.put(acc, opcode, %{opcode: opcode, name: name, request_extras: request_extras, response_extras: response_extras})
  end)

  @info_by_name Enum.reduce(@info, %{}, fn spec, acc ->
    {opcode, name, request_extras, response_extras} = spec
    Map.put(acc, name, %{opcode: opcode, name: name, request_extras: request_extras, response_extras: response_extras})
  end)

  def new(opcode, options \\ []) do
    info = info(opcode)

    header_options = Keyword.take(options, @header_keys)
    |> Map.new

    body_options = Keyword.take(options, @body_keys)
    |> Map.new

    %__MODULE__{
      info: info,
      header: Map.merge(@default_request_header, header_options),
      body: Map.merge(@default_request_body, body_options)
    }
  end

  def send(conn, opcode, options \\ []) do
    iodata = new(opcode, options) |> serialize()
    Cream.Connection.send(conn, iodata)
  end

  def recv(conn) do
    with {:ok, data} <- Cream.Connection.recv(conn, 24),
      %{header: %{total_body_length: size}} = packet when size > 0 <- deserialize_header(data),
      {:ok, data} <- Cream.Connection.recv(conn, size)
    do
      {:ok, deserialize_body(packet, data)}
    else
      %{header: %{total_body_length: 0}} = packet -> {:ok, packet}
      error -> error
    end
  end

  def serialize(packet) do
    import Cream.Utils, only: [bytes: 1]

    info = packet.info
    header = packet.header
    body = packet.body

    key = body.key || ""
    value = body.value || ""
    extras = serialize_extras(body.extras, info.request_extras)

    key_length    = byte_size(key)
    extras_length = IO.iodata_length(extras)
    value_length  = byte_size(value)

    total_body_length = key_length + extras_length + value_length

    [
      <<header.magic          :: bytes(1)>>,
      <<info.opcode  :: bytes(1)>>,
      <<key_length            :: bytes(2)>>,
      <<extras_length         :: bytes(1)>>,
      <<header.data_type      :: bytes(1)>>,
      <<header.vbucket_id     :: bytes(2)>>,
      <<total_body_length     :: bytes(4)>>,
      <<header.opaque         :: bytes(4)>>,
      <<header.cas            :: bytes(8)>>,

      extras,
      key,
      value
    ]
  end

  defp serialize_extras(extras, extras_formats) do
    Enum.map extras_formats, fn {name, size} ->
      bits = size * 8
      value = extras[name] || 0
      <<value :: size(bits) >>
    end
  end

  def deserialize_header(data) do
    import Cream.Utils, only: [bytes: 1]

    <<
      @response_magic   :: bytes(1),
      opcode            :: bytes(1),
      key_length        :: bytes(2),
      extras_length     :: bytes(1),
      data_type         :: bytes(1),
      status            :: bytes(2),
      total_body_length :: bytes(4),
      opaque            :: bytes(4),
      cas               :: bytes(8)
    >> = data

    header = %{
      magic:              @response_magic,
      opcode:             opcode,
      key_length:         key_length,
      extras_length:      extras_length,
      data_type:          data_type,
      status:             status,
      total_body_length:  total_body_length,
      opaque:             opaque,
      cas:                cas
    }

    %__MODULE__{
      info: info(opcode),
      header: header
    }
  end

  def deserialize_body(packet, data) do
    response_extras = packet.info.response_extras
    extras_length = packet.header.extras_length
    key_length = packet.header.key_length

    <<
      extras :: binary-size(extras_length),
      key :: binary-size(key_length),
      value :: binary
    >> = data

    body = %{
      extras: deserialize_extras(extras, response_extras),
      key: if(key == "", do: nil, else: key),
      value: if(value == "", do: nil, else: value)
    }

    %{packet | body: body}
  end

  defp deserialize_extras(data, extras_formats) do
    deserialize_extras(data, extras_formats, [])
  end

  defp deserialize_extras("", _extras_formats, acc), do: acc
  defp deserialize_extras(data, [{name, size} | extras_formats], acc) do
    bits = size * 8
    <<value :: size(bits), data :: binary>> = data
    deserialize_extras(data, extras_formats, [{name, value} | acc])
  end

  defp info(opcode) when is_integer(opcode) do
    @info_by_opcode[opcode]
  end

  defp info(name) when is_atom(name) do
    @info_by_name[name]
  end
end
