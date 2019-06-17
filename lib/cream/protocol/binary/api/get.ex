defmodule Cream.Protocol.Binary.Api.Get do

  alias Cream.Protocol.Binary.{Wire, Status}

  def call(conn, key, options \\ []) do
    with :ok <- Wire.Get.send(conn, key: key),
      {:ok, packet} <- Wire.recv(conn)
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
