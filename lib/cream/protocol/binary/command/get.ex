defmodule Cream.Protocol.Binary.Command.Get do

  alias Cream.Protocol.Binary.{Packet, Status}

  def call(conn, key) do
    with :ok <- Packet.send(conn, op: :get, key: key),
      {:ok, packet} <- Packet.recv(conn)
    do
      case Status.to_atom(packet.header.status) do
        nil -> packet.value
        :not_found -> nil
      end
    end
  end

end
