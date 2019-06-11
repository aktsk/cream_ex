defmodule Cream.Protocol.Binary.Api.Set do

  alias Cream.Protocol.Binary.{Wire, Status}

  @defaults [
    ttl: 0,
    flags: 0
  ]

  def call(conn, key, value, options) do
    options = Keyword.merge(@defaults, options)
    ttl = options[:ttl]
    flags = options[:flags]

    with :ok <- Wire.Set.send(conn, key: key, value: value, extras: [expiration: ttl, flags: flags]),
      {:ok, packet} <- Wire.recv(conn)
    do
      case Status.to_atom(packet.header.status) do
        nil -> :ok
        status -> {:error, status}
      end
    end
  end

end
