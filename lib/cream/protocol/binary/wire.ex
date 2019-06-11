defmodule Cream.Protocol.Binary.Wire do

  defmacro __using__(_opts) do
    quote do
      @opcode nil
      @request []
      @response []

      @before_compile Cream.Protocol.Binary.Wire
    end
  end

  defmacro __before_compile__(_env) do
    quote do

      @specification %{
        opcode: @opcode,
        request: %{
          extras: @request[:extras] || []
        },
        response: %{
          extras: @response[:extras] || []
        }
      }

      # Compile time check to make sure the opcode map is in sync with the wire protocol.
      if Cream.Protocol.Binary.Opcode.get_module(@opcode) != __MODULE__ do
        opcode_module = inspect(Cream.Protocol.Binary.Opcode)
        self_module = inspect(__MODULE__)
        raise "You forgot to add #{self_module} to the @opcode_map in #{opcode_module}"
      end

      def specification do
        @specification
      end

      def new_packet(options \\ []) do
        Cream.Protocol.Binary.Wire.new_packet(__MODULE__, options)
      end

      def send(conn, options \\ []) do
        Cream.Protocol.Binary.Wire.send(__MODULE__, conn, options)
      end

    end
  end

  alias Cream.Protocol.Binary.Packet

  def new_packet(module, options) do
    specification = module.specification
    options = Map.new(options) |> Map.put(:opcode, specification.opcode)

    Packet.new(
      header: Map.take(options, [:opcode, :data_type, :vbucket_id, :opaque, :cas]),
      body: Map.take(options, [:key, :value, :extras])
    )
  end

  def send(module, conn, options) do
    iodata = module.new_packet(options) |> Packet.serialize()
    Cream.Connection.send(conn, iodata)
  end

  def recv(conn) do
    with {:ok, data} <- Cream.Connection.recv(conn, 24),
      %{total_body_length: size} = header when size > 0 <- Packet.deserialize_header(data),
      {:ok, data} <- Cream.Connection.recv(conn, size)
    do
      body = Packet.deserialize_body(header, data)
      packet = %Packet{header: header, body: body}
      {:ok, packet}
    else
      %{total_body_length: 0} = header -> {:ok, %Packet{header: header}}
      error -> error
    end
  end

end
