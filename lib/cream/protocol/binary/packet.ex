defmodule Cream.Protocol.Binary.Packet do

  defstruct [:header, :body]

  @request_magic 0x80
  @response_magic 0x81

  @default_request_header %{
    magic: @request_magic,
    data_type: 0,
    vbucket_id: 0,
    opaque: 0,
    cas: 0
  }

  @default_request_body %{
    key: nil,
    value: nil,
    extras: []
  }

  alias Cream.Protocol.Binary.Opcode

  def new(options \\ []) do
    header_options = options[:header] || %{}
    body_options = options[:body] || %{}

    %__MODULE__{
      header: Map.merge(@default_request_header, header_options),
      body: Map.merge(@default_request_body, body_options)
    }
  end

  def serialize(packet) do
    import Cream.Utils, only: [bytes: 1]

    header = packet.header
    body = packet.body

    extras_formats = Opcode.get_specification(header.opcode).request.extras

    key = body.key || ""
    value = body.value || ""
    extras = serialize_extras(body.extras, extras_formats)

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

    %{
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
  end

  def deserialize_body(header, data) do
    specification = Opcode.get_specification(header.opcode)
    deserialize_body(header, data, specification.response.extras)
  end

  def deserialize_body(header, data, extras_formats) do
    extras_length = header.extras_length
    key_length = header.key_length

    <<
      extras :: binary-size(extras_length),
      key :: binary-size(key_length),
      value :: binary
    >> = data

    %{
      extras: deserialize_extras(extras, extras_formats),
      key: if(key == "", do: nil, else: key),
      value: if(value == "", do: nil, else: value)
    }
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

end
