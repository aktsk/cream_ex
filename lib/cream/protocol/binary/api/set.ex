defmodule Cream.Protocol.Binary.Api.Set do

  alias Cream.Protocol.Binary.{Packet, Status}

  @defaults [
    ttl: 0,
    flags: 0,
    cas: 0
  ]

  def call(conn, key, value, options) do
    options = Keyword.merge(@defaults, options)

    args = [
      key: key,
      value: value,
      extras: [
        expiration: options[:ttl],
        flags: options[:flags]
      ],
      cas: options[:cas]
    ]

    with :ok <- Packet.send(conn, :set, args),
      {:ok, packet} <- Packet.recv(conn)
    do
      case Status.to_atom(packet.header.status) do
        nil -> :ok
        status -> {:error, status}
      end
    end
  end

end
