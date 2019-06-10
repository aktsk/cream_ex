defmodule Cream.Protocol.Binary.Packet do

  defstruct [:header, :body]

  defmacro __using__(_opts) do
    quote do
      @opcode nil
      @request []
      @response []

      @before_compile Cream.Protocol.Binary.Packet
    end
  end

  defmacro __before_compile__(_env) do
    quote do

      @specification %{
        module: __MODULE__,
        opcode: @opcode,
        request: %{
          extras: @request[:extras] || []
        },
        response: %{
          extras: @response[:extras] || []
        }
      }

      def specification do
        @specification
      end

      def new(options \\ []) do
        Cream.Protocol.Binary.Packet.new(@specification, options)
      end

      def send(conn, options \\ []) do
        Cream.Protocol.Binary.Packet.send(conn, @specification, options)
      end

      def recv(conn) do
        Cream.Protocol.Binary.Packet.recv(conn, @specification)
      end
    end
  end

  @request_magic 0x80
  @response_magic 0x81

  def send(conn, specification, options) do
    iodata = new(specification, options) |> serialize()
    Cream.Connection.send(conn, iodata)
  end

  def recv(conn, specification) do
    with {:ok, header} <- deserialize_header(conn),
      {:ok, body} <- deserialize_body(conn, specification, header)
    do
      packet = %__MODULE__{header: header, body: body}
      {:ok, packet}
    end
  end

  def recv(conn) do
    with {:ok, header} <- deserialize_header(conn),
      {:ok, body} <- deserialize_body(conn, header)
    do
      packet = %__MODULE__{header: header, body: body}
      {:ok, packet}
    end
  end

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

  def new(specification, options) do
    options = Map.new(options)

    header_options = Map.take(options, [:data_type, :vbucket_id, :opaque, :cas])
    body_options = Map.take(options, [:key, :value, :extras])

    header = Map.merge(@default_request_header, header_options)
    |> Map.put(:opcode, specification.opcode)

    body = Map.merge(@default_request_body, body_options)

    extras = Enum.map specification.request.extras, fn {name, size} ->
      value = body.extras[name] || 0
      {name, size, value}
    end

    body = put_in(body.extras, extras)

    %__MODULE__{header: header, body: body}
  end

  def serialize(packet) do
    import Cream.Utils, only: [bytes: 1]

    header = packet.header
    body = packet.body

    key = body.key || ""
    value = body.value || ""
    extras = serialize_extras(body.extras)

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

  defp serialize_extras(extras) do
    Enum.map extras, fn {_name, size, value} ->
      bits = size * 8
      <<value :: size(bits) >>
    end
  end

  defp deserialize_header(conn) do
    import Cream.Utils, only: [bytes: 1]

    with {:ok, data} <- Cream.Connection.recv(conn, 24) do
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

      {:ok, header}
    end
  end

  defp deserialize_body(conn, header) do
    module = Cream.Protocol.Binary.Opcode.get_module(header.opcode)
    deserialize_body(conn, module.specification, header)
  end

  defp deserialize_body(_conn, _specification, %{total_body_length: 0}), do: {:ok, nil}
  defp deserialize_body(conn, specification, header) do
    with {:ok, data} <- Cream.Connection.recv(conn, header.total_body_length) do
      extras_length = header.extras_length
      key_length = header.key_length

      <<
        extras :: binary-size(extras_length),
        key :: binary-size(key_length),
        value :: binary
      >> = data

      body = %{
        extras: deserialize_extras(specification, extras),
        key: if(key == "", do: nil, else: key),
        value: if(value == "", do: nil, else: value)
      }

      {:ok, body}
    end
  end

  defp deserialize_extras(specification, data) do
    deserialize_extras(specification.response.extras, data, [])
  end

  defp deserialize_extras(_extras_specification, "", acc), do: acc
  defp deserialize_extras([{name, size} | extras_specification], data, acc) do
    bits = size * 8
    <<value :: size(bits), data :: binary>> = data
    deserialize_extras(extras_specification, data, [{name, value} | acc])
  end

end
