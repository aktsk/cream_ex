defmodule Cream.Protocol.Binary.Command.Get do

  alias Cream.Protocol.Binary.{Packet, Status}

  def call(conn, key) do
    with :ok <- Packet.Get.send(conn, key: key),
      {:ok, packet} <- Packet.Get.recv(conn)
    do
      case Status.to_atom(packet.header.status) do
        nil -> packet.body.value
        :not_found -> nil
      end
    end
  end

end
