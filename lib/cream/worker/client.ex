defmodule Cream.Worker.Client do
  defstruct [:options, :socket]
  use GenServer
  require Logger

  def start_link(options) do
    GenServer.start_link(__MODULE__, options)
  end

  def init(options) do
    case establish_connection(options) do
      {:ok, socket} ->
        {:ok, %__MODULE__{options: options, socket: socket}}
      {:error, _reason} ->
        {:connect, nil, %__MODULE__{options: options}}
    end
  end

  def handle_call({:store, "mset", keys_and_values, options}, _from, state) do
    options = Keyword.put(options, :noreply, true)

    command =
      keys_and_values
      |> Stream.map(fn {key, value} -> build_store_command("set", key, value, options) end)
      |> Enum.join

    socket_send(state.socket, command)

    {:reply, :ok, state}
  end

  def handle_call({:store, cmd, {key, value}, options}, _from, state) do
    command = build_store_command(cmd, key, value, options)
    socket_send(state.socket, command)
    response = recv_line(state.socket) |> atomize_line

    {:reply, response, state}
  end

  def handle_call({:get, keys, options}, _from, state) do
    retrieve("get", keys, options, state)
  end

  def handle_call({:gets, keys, options}, _from, state) do
    retrieve("gets", keys, options, state)
  end

  def handle_call({:delete, key}, _from, state) do
    socket_send(state.socket, "delete #{key}\r\n")
    response = recv_line(state.socket) |> atomize_line
    {:reply, response, state}
  end

  def handle_call({:flush_all, delay}, _from, state) do
    socket_send(state.socket, "flush_all #{delay}\r\n")
    response = recv_line(state.socket) |> atomize_line
    {:reply, response, state}
  end

  defp establish_connection(options) do
    [host, port] = options[:server] |> String.split(":")

    host = String.to_charlist(host)
    port = String.to_integer(port)

    case :gen_tcp.connect(host, port, [:binary, active: false]) do
      {:error, reason} = value ->
        Logger.warn("ERROR: establishing connection to #{options[:server]}: #{reason}")
        value
      value -> value
    end
  end

  defp retrieve(cmd, keys, options, state) do
    options = Keyword.merge(state.options, options)
    keys_string = Enum.join(keys, " ")
    socket_send(state.socket, "#{cmd} #{keys_string}\r\n")
    {:reply, recv_values(state.socket, options[:coder]), state}
  end

  defp build_store_command(cmd, key, value, options) do
    {flags, value} = encode(value, options[:coder])
    exptime = options[:ttl]
    bytes = byte_size(value)

    command = "#{cmd} #{key} #{flags} #{exptime} #{bytes}"

    command = if options[:cas] do
      "#{command} #{options[:cas]}"
    else
      command
    end

    command = if options[:noreply] do
      "#{command} noreply"
    else
      command
    end

    "#{command}\r\n#{value}\r\n"
  end

  defp recv_values(socket, coder, values \\ %{}) do
    with {:ok, line} <- recv_line(socket),
      {:ok, key, flags, value} <- recv_value(socket, line)
    do
      value = decode(flags, value, coder)
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

  defp recv_line(socket) do
    :ok = :inet.setopts(socket, packet: :line)
    case :gen_tcp.recv(socket, 0) do
      {:ok, line} -> {:ok, chomp(line)}
      error -> error
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

  defp socket_send(socket, data) do
    :ok = :gen_tcp.send(socket, data)
  end

  defp chomp(line) do
    String.replace_suffix(line, "\r\n", "")
  end

  defp atomize_line({:ok, line}), do: {:ok, line |> String.downcase |> String.to_atom}
  defp atomize_line(error), do: error

  defp encode(value, nil), do: {0, value}
  defp encode(value, coder), do: coder.encode(value)

  defp decode(_flags, value, nil), do: value
  defp decode(flags, value, coder) do
    flags
    |> String.to_integer
    |> coder.decode(value)
  end

end
