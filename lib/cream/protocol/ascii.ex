defmodule Cream.Protocol.Ascii do

  alias Cream.{Coder, Utils}
  alias Cream.Protocol.Reason

  def set(socket, keys_and_values, options) do
    build_store_commmands("set", keys_and_values, options)
    |> socket_send(socket)

    keys_and_values
    |> Utils.stream_keys
    |> keyed_response(socket, :set)
  end

  # def add(socket, {key, value}, options) do
  #   add(socket, [{key, value}], options)
  #   |> response_for(key)
  # end
  #
  # def add(socket, keys_and_values, options) do
  #   build_store_commmands("add", keys_and_values, options)
  #   |> socket_send(socket)
  #
  #   multi_response(keys_and_values, {:ok, "STORED"}, :stored, socket)
  # end
  #
  # def replace(socket, {key, value}, options) do
  #   replace(socket, [{key, value}], options)
  #   |> response_for(key)
  # end
  #
  # def replace(socket, keys_and_values, options) do
  #   build_store_commmands("replace", keys_and_values, options)
  #   |> socket_send(socket)
  #
  #   multi_response(keys_and_values, {:ok, "STORED"}, :stored, socket)
  # end
  
  def get(socket, keys, options) do
    opcode = if options[:cas], do: :gets, else: :get
    Enum.reduce(keys, [opcode], &append(&2, &1))
    |> append("\r\n", :trim)
    |> socket_send(socket)

    Stream.repeatedly(fn -> recv_message(socket, opcode) end)
    |> Stream.take_while(fn
      {:ok, :end} -> false
      _ -> true
    end)
    |> Enum.reduce({:ok, %{}}, fn message, {status, acc} ->

    end)
  end

  # def delete(socket, key, options) when not is_list(key) do
  #   delete(socket, [key], options)
  #   |> response_for(key)
  # end
  #
  # def delete(socket, keys, _options) do
  #   Enum.map(keys, &"delete #{&1}\r\n")
  #   |> socket_send(socket)
  #
  #   multi_response(keys, {:ok, "DELETED"}, :deleted, socket)
  # end

  def flush(socket, options) do
    ["flush_all"]
    |> append(options[:delay])
    |> append("\r\n", :trim)
    |> socket_send(socket)

    recv_message(socket, :flush)
  end

  def keyed_response(keys, socket, opcode) do
    Enum.reduce keys, {:ok, %{}}, fn key, {status, acc} ->
      case recv_message(socket, opcode) do
        {:ok, result} ->
          Map.update(acc, result, [key], &[key | &1])
          |> Utils.return_tuple(status)
        {:error, reason} ->
          Map.update(acc, :errors, %{reason => [key]}, fn errors ->
            Map.update(errors, reason, [key], &[key | &1])
          end)
          |> Utils.return_tuple(:error)
      end
    end
  end

  @opcodes [:get, :gets]
  defp recv_message(socket, opcode) when opcode in @opcodes do
    with {:ok, line} <- recv_line(socket),
      {key, flags, byte_size, cas} = parse_value_line(line),
      {:ok, value} <- recv_value(socket, byte_size)
    do
      {:ok, {key, value, flags, cas}}
    end
  end

  @opcodes [:set, :add, :replace, :delete, :flush]
  defp recv_message(socket, opcode) when opcode in @opcodes do
    with {:ok, line} <- recv_line(socket) do
      case {opcode, line} do
        {:add,      "NOT_STORED\r\n"} -> {:ok, :exists   }
        {:replace,  "NOT_STORED\r\n"} -> {:ok, :not_found}
        {:delete,   "DELETED\r\n"   } -> {:ok, :deleted  }
        {:delete,   "NOT_FOUND\r\n" } -> {:ok, :not_found}
        {:flush,    "OK\r\n"        } -> {:ok, :flushed  }
        {_,         "STORED\r\n"    } -> {:ok, :stored   }
      end
    end
  end

  defp parse_value_line(line) do
    case line |> chomp |> String.split(" ") do
      ["END"] -> :end
      ["VALUE", key, flags, byte_size] -> {key, flags, byte_size, nil}
      ["VALUE", key, flags, byte_size, cas] -> {key, flags, byte_size, cas}
    end
  end


  # def multi_response(keys, success_case, success_reason, socket) do
  #   errors = Enum.reduce(keys, %{}, fn key, acc ->
  #
  #
  #     key = with {key, _value} <- item, do: key # item is either a {key, value} or just a key.
  #     case recv_line(socket) do
  #       ^success_case -> acc
  #       {_status, reason} -> Map.put(acc, key, Reason.tr(reason))
  #     end
  #   end)
  #
  #   if errors == %{} do
  #     {:ok, success_reason}
  #   else
  #     {:error, errors}
  #   end
  # end

  # defp response_for(response, key) do
  #   with {status, %{^key => value}} <- response do
  #     {status, value}
  #   end
  # end

  defp build_store_commmands(cmd, keys_and_values, options) do
    keys_and_values
    |> Enum.map(fn {key, value} -> build_store_command(cmd, key, value, options) end)
  end

  defp build_store_command(cmd, key, value, options) do
    # Value can be a tuple {value, cas} or just a value.
    {value, cas} = with value when is_binary(value) <- value, do: {value, nil}

    cmd = if cmd == "set" && cas do
      "cas"
    else
      cmd
    end

    {flags, value} = Coder.encode(options[:coder], value)
    exptime = options[:ttl]
    bytes = byte_size(value)

    [cmd]
    |> append(key)
    |> append(flags)
    |> append(exptime)
    |> append(bytes)
    |> append(cas)
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
      case line do
        <<"SERVER_ERROR ", reason::binary>> -> {:error, chomp(reason)}
        <<"CLIENT_ERROR ", reason::binary>> -> swallow_error_line(socket, chomp(reason))
        "ERROR\r\n" -> {:error, :unknown}
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

  # defp recv_values(socket, coder, values \\ %{}) do
  #   with {:ok, line} <- recv_line(socket),
  #     {:ok, key, flags, value} <- recv_value(socket, line)
  #   do
  #     value = Coder.decode(coder, flags, value)
  #     values = Map.put(values, key, value)
  #     recv_values(socket, coder, values)
  #   else
  #     :end -> {:ok, values}
  #     error -> error
  #   end
  # end

  # defp recv_value(socket, byte_size) do
  #   with {:ok, value} <- recv_bytes(socket, byte_size) do
  #     {:ok, chomp(value)}
  #   end
  # end

  # defp recv_value(socket, line) do
  #   case line |> chomp |> String.split(" ") do
  #     ["END"] -> :end
  #     ["VALUE", key, flags, bytes, cas] ->
  #       case recv_bytes(socket, bytes) do
  #         {:ok, value} -> {:ok, key, flags, {value, String.to_integer(cas)}}
  #         error -> error
  #       end
  #     ["VALUE", key, flags, bytes] ->
  #       case recv_bytes(socket, bytes) do
  #         {:ok, value} -> {:ok, key, flags, value}
  #         error -> error
  #       end
  #   end
  # end

  defp recv_value(socket, n) do
    :ok = :inet.setopts(socket, packet: :raw)
    n = String.to_integer(n)
    with {:ok, data} <- :gen_tcp.recv(socket, n + 2), do: {:ok, chomp(data)}
  end

  defp socket_send(data, socket) do
    :ok = :gen_tcp.send(socket, data)
  end

end
