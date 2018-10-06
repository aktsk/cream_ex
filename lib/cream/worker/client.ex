defmodule Cream.Worker.Client do
  defstruct [:options, :socket]
  use GenServer
  require Logger

  alias Cream.Protocol

  def start_link(options) do
    GenServer.start_link(__MODULE__, options)
  end

  @defaults [
    protocol: Protocol.Binary,
    ttl: 0
  ]
  def init(options) do
    options = Keyword.merge(@defaults, options)

    case establish_connection(options) do
      {:ok, socket} ->
        {:ok, %__MODULE__{options: options, socket: socket}}
      {:error, _reason} ->
        {:connect, nil, %__MODULE__{options: options}}
    end
  end

  def handle_call({:set, keys_and_values, options}, _from, state) do
    protocol(:set, [keys_and_values], options, state)
  end

  # def handle_call({:store, "mset", keys_and_values, options}, _from, state) do
  #   options = Keyword.put(options, :noreply, true)
  #
  #   command =
  #     keys_and_values
  #     |> Stream.map(fn {key, value} -> build_store_command("set", key, value, options) end)
  #     |> Enum.join
  #
  #   socket_send(state.socket, command)
  #
  #   {:reply, :ok, state}
  # end
  #
  # def handle_call({:store, cmd, {key, value}, options}, _from, state) do
  #   command = build_store_command(cmd, key, value, options)
  #   socket_send(state.socket, command)
  #   response = recv_line(state.socket) |> atomize_line
  #
  #   {:reply, response, state}
  # end

  def handle_call({:get, keys, options}, _from, state) do
    protocol(:get, [keys], options, state)
  end

  def handle_call({:flush, options}, _from, state) do
    protocol(:flush, [], options, state)
  end

  # def handle_call({:gets, keys, options}, _from, state) do
  #   # retrieve("gets", keys, options, state)
  #   {:reply, Cream.Protocol.Binary.mget(state.socket, keys), state}
  # end
  #
  # def handle_call({:delete, key}, _from, state) do
  #   socket_send(state.socket, "delete #{key}\r\n")
  #   response = recv_line(state.socket) |> atomize_line
  #   {:reply, response, state}
  # end
  #
  # def handle_call({:flush_all, delay}, _from, state) do
  #   socket_send(state.socket, "flush_all #{delay}\r\n")
  #   response = recv_line(state.socket) |> atomize_line
  #   {:reply, response, state}
  # end

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

  defp encode(value, nil), do: {0, value}
  defp encode(value, coder), do: coder.encode(value)

  defp decode(_flags, value, nil), do: value
  defp decode(flags, value, coder) do
    flags
    |> String.to_integer
    |> coder.decode(value)
  end

  defp protocol(func_name, args, options, state) do
    options = Keyword.merge(state.options, options)

    module = case options[:protocol] do
      :ascii  -> Protocol.Ascii
      :text   -> Protocol.Ascii
      :binary -> Protocol.Binary
      module  -> module
    end

    args = [state.socket] ++ args ++ [options]

    {:reply, apply(module, func_name, args), state}
  end

end
