defmodule Cream.Protocol.Ascii do

  alias Cream.Coder
  alias Cream.Protocol.Reason

  def flush(socket, options) do
    ["flush_all"]
    |> cmd(options[:delay])
    |> cmd("\r\n", :trim)
    |> socket_send(socket)

    recv_line(socket)
  end

  def set(socket, {key, value}, options) do
    build_store_command("set", key, value, options)
    |> socket_send(socket)

    recv_line(socket)
  end

  def set(socket, keys_and_values, options) do
    keys_and_values
    |> Enum.map(fn {key, value} -> build_store_command("set", key, value, options) end)
    |> socket_send(socket)

    errors = Enum.reduce(keys_and_values, %{}, fn {key, _value}, acc ->
      case recv_line(socket) do
        {:error, reason} -> Map.put(acc, key, reason)
        _ -> acc
      end
    end)

    if errors == %{} do
      {:ok, :stored}
    else
      {:error, errors}
    end
  end

  def add(socket, {key, value}, options) do
    build_store_command("add", key, value, options)
    |> socket_send(socket)

    recv_line(socket)
  end

  def get(socket, keys, options) when is_list(keys) do
    Enum.reduce(keys, ["get"], &cmd(&2, &1))
    |> cmd("\r\n", :trim)
    |> socket_send(socket)

    recv_values(socket, options[:coder])
  end

  def get(socket, key, options) do
    case get(socket, [key], options) do
      {:ok, values} -> {:ok, values[key]}
      error -> error
    end
  end

  defp build_store_command(cmd, key, value, options) do
    {flags, value} = Coder.encode(options[:coder], value)
    exptime = options[:ttl]
    bytes = byte_size(value)

    [cmd]
    |> cmd(key)
    |> cmd(flags)
    |> cmd(exptime)
    |> cmd(bytes)
    |> cmd(options[:cas])
    |> cmd(options[:noreply] && "noreply")
    |> cmd("\r\n", :trim)
    |> cmd(value, :trim)
    |> cmd("\r\n", :trim)
  end

  defp cmd(command, arg, trim \\ nil)
  defp cmd(command, nil, _trim), do: command
  defp cmd(command, "", _trim), do: command
  defp cmd(command, arg, nil), do: [command, " ", to_string(arg)]
  defp cmd(command, arg, :trim), do: [command, to_string(arg)]

  defp chomp(line), do: String.replace_suffix(line, "\r\n", "")

  defp recv_line(socket) do
    :ok = :inet.setopts(socket, packet: :line)
    with {:ok, line} = :gen_tcp.recv(socket, 0) do
      case chomp(line) do
        <<"SERVER_ERROR ", reason::binary>> -> {:error, reason}
        <<"CLIENT_ERROR ", reason::binary>> ->
          # I think this is a bug in memcached; any CLIENT_ERROR <reason>\r\n is followed by
          # an ERROR\r\n. This is not the case for SERVER_ERROR <reason>\r\n lines.
          case :gen_tcp.recv(socket, 0) do
            {:ok, "ERROR\r\n"} -> {:error, reason}
            error -> error
          end
        "STORED"      -> {:ok,    Reason.tr("STORED")}
        "NOT_STORED"  -> {:error, Reason.tr("NOT_STORED")}
        "EXISTS"      -> {:error, Reason.tr("EXISTS")}
        "NOT_FOUND"   -> {:error, Reason.tr("NOT_FOUND")}
        line -> {:ok, line}
      end
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
