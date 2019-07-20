defmodule Cream.Protocol.Binary.Api.Mget do

  alias Cream.Protocol.Binary.{Packet, Status}

  def call(conn, keys, options \\ []) do
    with :ok <- send_requests(conn, keys),
      :ok <- Packet.send(conn, :noop)
    do
      recv_responses(conn, options)
    end
  end

  defp send_requests(conn, keys) do
    Enum.reduce_while(keys, :ok, fn key, _acc ->
      case Packet.send(conn, :getkq, key: key) do
        :ok -> {:cont, :ok}
        error -> {:halt, error}
      end
    end)
  end

  defp recv_responses(conn, options) do
    Stream.repeatedly(fn -> Packet.recv(conn) end)
    |> Enum.reduce_while([], fn
      {:error, _error} = error, _acc -> {:halt, error}
      {:ok, packet}, acc -> if packet.info.name == :noop do
        {:halt, {:ok, Enum.reverse(acc)}}
      else
        case Status.to_atom(packet.header.status) do
          nil ->
            item = {packet.body.key, value(packet, options)}
            {:cont, [item | acc]}
          error ->
            item = {packet.body.key, {:error, error}}
            {:cont, [item | acc]}
        end
      end
    end)
  end

  defp value(packet, options) do
    if options[:cas] do
      {packet.body.value, packet.header.cas}
    else
      packet.body.value
    end
  end

end
