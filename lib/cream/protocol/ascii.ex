defmodule Cream.Protocol.Ascii do

  alias Cream.Coder
  alias Cream.Protocol.Reason

  def flush(socket, options) do
    ["flush_all"]
    |> append(options[:delay])
    |> append("\r\n", :trim)
    |> socket_send(socket)

    with {:ok, "OK"} <- recv_line(socket), do: {:ok, :flushed}
  end

  def set(socket, {key, value}, options) do
    set(socket, [{key, value}], options)
    |> response_for(key)
  end

  def set(socket, keys_and_values, options) do
    build_store_commmands("set", keys_and_values, options)
    |> socket_send(socket)

    multi_response(keys_and_values, {:ok, "STORED"}, :stored, socket)
  end

  def add(socket, {key, value}, options) do
    add(socket, [{key, value}], options)
    |> response_for(key)
  end

  def add(socket, keys_and_values, options) do
    build_store_commmands("add", keys_and_values, options)
    |> socket_send(socket)

    multi_response(keys_and_values, {:ok, "STORED"}, :stored, socket)
  end

  def replace(socket, {key, value}, options) do
    replace(socket, [{key, value}], options)
    |> response_for(key)
  end

  def replace(socket, keys_and_values, options) do
    build_store_commmands("replace", keys_and_values, options)
    |> socket_send(socket)

    multi_response(keys_and_values, {:ok, "STORED"}, :stored, socket)
  end

  def get(socket, keys, options) when is_list(keys) do
    Enum.reduce(keys, ["get"], &append(&2, &1))
    |> append("\r\n", :trim)
    |> socket_send(socket)

    recv_values(socket, options[:coder])
  end

  def get(socket, key, options) do
    case get(socket, [key], options) do
      {:ok, values} -> {:ok, values[key]}
      error -> error
    end
  end

  def delete(socket, key, options) when not is_list(key) do
    delete(socket, [key], options)
    |> response_for(key)
  end

  def delete(socket, keys, _options) do
    Enum.map(keys, &"delete #{&1}\r\n")
    |> socket_send(socket)

    multi_response(keys, {:ok, "DELETED"}, :deleted, socket)
  end

  def multi_response(items, success_case, success_reason, socket) do
    errors = Enum.reduce(items, %{}, fn item, acc ->
      key = with {key, _value} <- item, do: key # item is either a {key, value} or just a key.
      case recv_line(socket) do
        ^success_case -> acc
        {_status, reason} -> Map.put(acc, key, Reason.tr(reason))
      end
    end)

    if errors == %{} do
      {:ok, success_reason}
    else
      {:error, errors}
    end
  end

  defp response_for(response, key) do
    with {status, %{^key => value}} <- response do
      {status, value}
    end
  end

  defp build_store_commmands(cmd, keys_and_values, options) do
    keys_and_values
    |> Enum.map(fn {key, value} -> build_store_command(cmd, key, value, options) end)
  end

  defp build_store_command(cmd, key, value, options) do
    {flags, value} = Coder.encode(options[:coder], value)
    exptime = options[:ttl]
    bytes = byte_size(value)

    [cmd]
    |> append(key)
    |> append(flags)
    |> append(exptime)
    |> append(bytes)
    |> append(options[:cas])
    |> append(options[:noreply] && "noreply")
    |> append("\r\n", :trim)
    |> append(value, :trim)
    |> append("\r\n", :trim)
  end

  defp append(command, arg, trim \\ nil)
  defp append(command, nil, _trim), do: command
  defp append(command, "", _trim), do: command
  defp append(command, arg, nil), do: [command, " ", to_string(arg)]
  defp append(command, arg, :trim), do: [command, to_string(arg)]

  defp chomp(line), do: String.replace_suffix(line, "\r\n", "")

  defp recv_line(socket) do
    with :ok <- :inet.setopts(socket, packet: :line),
      {:ok, line} = :gen_tcp.recv(socket, 0)
    do
      case chomp(line) do
        <<"SERVER_ERROR ", reason::binary>> -> {:error, reason}
        <<"CLIENT_ERROR ", reason::binary>> -> swallow_error_line(socket, reason)
        "ERROR" -> {:error, :unknown}
        line -> {:ok, line}
      end
    end
  end

  # I think this is a bug in memcached; any CLIENT_ERROR <reason>\r\n is followed by
  # an ERROR\r\n. This is not the case for SERVER_ERROR <reason>\r\n lines.
  def swallow_error_line(socket, reason) do
    with {:ok, "ERROR\r\n"} <- :gen_tcp.recv(socket, 0) do
      {:error, reason}
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
