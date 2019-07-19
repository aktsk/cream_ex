defmodule Cream.Protocol.Binary.Api.Get do

  alias Cream.Protocol.Binary.{Packet, Status}

  def call(conn, key, options \\ []) do
    with :ok <- Packet.send(conn, :get, key: key),
      {:ok, packet} <- Packet.recv(conn)
    do
      case Status.to_atom(packet.header.status) do
        nil -> if options[:cas] do
          {packet.body.value, packet.header.cas}
        else
          packet.body.value
        end
        :not_found -> nil
      end
    end
  end

end
