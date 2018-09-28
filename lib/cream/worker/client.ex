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

  def handle_call({:store, cmd, key, value, options}, _from, state) do
    flags   = options[:flags] || 0
    exptime = options[:ttl]
    cas     = options[:cas]
    bytes   = byte_size(value)

    command = build_store_command(cmd, key, flags, exptime, bytes, cas, value)
    socket_send(state.socket, command)
    response = recv_line(state.socket) |> atomize_line

    {:reply, response, state}
  end

  def handle_call({:get, keys, options}, _from, state) do
    keys_string = Enum.join(keys, " ")
    socket_send(state.socket, "get #{keys_string}\r\n")
    {:reply, recv_values(state.socket), state}
  end

  def handle_call({:gets, keys, options}, _from, state) do
    keys_string = Enum.join(keys, " ")
    socket_send(state.socket, "gets #{keys_string}\r\n")
    {:reply, recv_values(state.socket), state}
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

  defp build_store_command("cas", key, flags, exptime, bytes, cas, value) do
    "cas #{key} #{flags} #{exptime} #{bytes} #{cas}\r\n#{value}\r\n"
  end

  defp build_store_command(cmd, key, flags, exptime, bytes, _cas, value) do
    "#{cmd} #{key} #{flags} #{exptime} #{bytes}\r\n#{value}\r\n"
  end

  defp recv_values(socket, values \\ %{}) do
    with {:ok, line} <- recv_line(socket),
      {:ok, key, value} <- recv_value(socket, line)
    do
      values = Map.put(values, key, value)
      recv_values(socket, values)
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
          {:ok, value} -> {:ok, key, {value, cas}}
          error -> error
        end
      ["VALUE", key, flags, bytes] ->
        case recv_bytes(socket, bytes) do
          {:ok, value} -> {:ok, key, value}
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

end
