defmodule Cream.Protocol.Ascii do

  alias Cream.Coder

  def flush(socket, options) do
    "flush_all #{options[:delay]}\r\n"
    |> socket_send(socket)

    recv_line(socket)
  end

  def set(socket, {key, value}, options) do
    build_store_command("set", key, value, options)
    |> socket_send(socket)

    recv_line(socket)
    |> atomize_line
  end

  def set(socket, keys_and_values, options) do
    keys_and_values
    |> Enum.map(fn {key, value} -> build_store_command("set", key, value, options) end)
    |> socket_send(socket)

    Enum.reduce(keys_and_values, %{}, fn {key, _value}, acc ->
      response = recv_line(socket) |> atomize_line
      Map.put(acc, key, response)
    end)
  end

  def get(socket, keys, options) when is_list(keys) do
    command = Enum.reduce(keys, ["get"], fn key, acc ->
      [acc, " ", key]
    end)

    [command, "\r\n"]
    |> socket_send(socket)

    case recv_values(socket, options[:coder]) do
      {:ok, values} -> Enum.reduce(keys, values, fn key, acc ->
        case values[key] do
          nil -> Map.put(acc, key, {:ok, nil})
          value -> Map.put(acc, key, {:ok, value})
        end
      end)
      error -> error
    end
  end

  def get(socket, key, options) do
    case get(socket, [key], options) do
      %{} = values -> values[key]
      error -> error
    end
  end

  defp build_store_command(cmd, key, value, options) do
    {flags, value} = Coder.encode(options[:coder], value)
    flags = flags |> to_string
    exptime = options[:ttl] |> to_string
    bytes = byte_size(value) |> to_string

    [cmd, " ", key, " ", flags, " ", exptime, " ", bytes]
    |> add_cas(options[:cas])
    |> add_noreply(options[:noreply])
    |> add_value(value)
  end

  defp add_cas(command, nil), do: command
  defp add_cas(command, cas), do: [command, " ", cas]

  defp add_noreply(command, true), do: [command, " noreply"]
  defp add_noreply(command, _), do: command

  defp add_value(command, value), do: [command, "\r\n", value, "\r\n"]

  defp chomp(line), do: String.replace_suffix(line, "\r\n", "")

  defp atomize_line({:ok, line}), do: {:ok, line |> String.downcase |> String.to_atom}
  defp atomize_line(error), do: error

  defp recv_line(socket) do
    :ok = :inet.setopts(socket, packet: :line)
    case :gen_tcp.recv(socket, 0) do
      {:ok, line} -> {:ok, chomp(line)}
      error -> error
    end
  end

  defp recv_values(socket, coder, values \\ %{}) do
    with {:ok, line} <- recv_line(socket),
      {:ok, key, flags, value} <- recv_value(socket, line)
    do
      value = Coder.decode(coder, flags, value)
      values = Map.put(values, key, value)
      recv_values(socket, coder, values)
    else
      :end -> {:ok, values}
      error -> error
    end
  end

  defp recv_value(socket, line) do
    case String.split(line, " ") do
      ["END"] -> :end
      ["VALUE", key, flags, bytes, cas] ->
        case recv_bytes(socket, bytes) do
          {:ok, value} -> {:ok, key, flags, {value, cas}}
          error -> error
        end
      ["VALUE", key, flags, bytes] ->
        case recv_bytes(socket, bytes) do
          {:ok, value} -> {:ok, key, flags, value}
          error -> error
        end
    end
  end

  defp recv_bytes(socket, n) do
    :ok = :inet.setopts(socket, packet: :raw)
    n = String.to_integer(n)
    case :gen_tcp.recv(socket, n + 2) do
      {:ok, data} -> {:ok, chomp(data)}
      error -> error
    end
  end

  defp socket_send(data, socket) do
    :ok = :gen_tcp.send(socket, data)
  end

end
