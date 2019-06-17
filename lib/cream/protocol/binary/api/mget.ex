defmodule Cream.Protocol.Binary.Api.Mget do

  alias Cream.Protocol.Binary.{Wire, Status}

  def call(conn, keys, options \\ []) do
    with :ok <- send_requests(conn, keys),
      :ok <- Wire.Noop.send(conn),
      {:ok, results} <- recv_responses(conn, options)
    do
      results
    end
  end

  defp send_requests(conn, keys) do
    Enum.reduce_while(keys, :ok, fn key, _acc ->
      case Wire.Getkq.send(conn, key: key) do
        :ok -> {:cont, :ok}
        error -> {:halt, error}
      end
    end)
  end

  defp recv_responses(conn, options) do
    Stream.repeatedly(fn -> Wire.recv(conn) end)
    |> Enum.reduce_while(%{}, fn
      {:error, _error}, _acc = error -> {:halt, error}
      {:ok, packet}, acc -> if packet.header.opcode == Wire.Noop.opcode do
        {:halt, acc}
      else
        case Status.to_atom(packet.header.status) do
          nil -> {:cont, Map.put(acc, packet.body.key, value(packet, options))}
          error -> {:cont, Map.put(acc, packet.body.key, {:error, error})}
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
