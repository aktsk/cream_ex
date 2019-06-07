defmodule Cream.Worker.Connection do
  defstruct [:options, :socket, :socket_error, :protocol]

  use Connection
  require Logger
  alias Cream.Protocol

  def start_link(options, gs_options) do
    Connection.start_link(__MODULE__, options, gs_options)
  end

  @defaults [
    protocol: Protocol.Binary,
    ttl: 0
  ]
  def init(options) do
    options = Keyword.merge(@defaults, options)

    protocol = case options[:protocol] do
      :ascii  -> Protocol.Ascii
      :text   -> Protocol.Ascii
      :binary -> Protocol.Binary
      module  -> module
    end

    {:connect, :init, %__MODULE__{
      options: options,
      protocol: protocol
    }}
  end

  def connect(_info, state) do
    server = state.options[:server]

    case establish_connection(server) do
      {:ok, socket} ->
        {:ok, %{state | socket: socket, socket_error: nil}}
      {:error, reason} ->
        {:backoff, 1000, %{state | socket_error: reason}}
    end
  end

  def handle_call(:debug, _from, state) do
    {:reply, state, state}
  end

  def handle_call(:options, _from, state) do
    {:reply, state.options, state}
  end

  def handle_call(_args, _from, %{socket: nil} = state) do
    {:reply, {:error, state.socket_error}, state}
  end

  def handle_call({:set, keys_and_values, options}, _from, state) do
    protocol(:set, [keys_and_values], options, state)
  end

  def handle_call({:add, keys_and_values, options}, _from, state) do
    protocol(:add, [keys_and_values], options, state)
  end

  def handle_call({:replace, keys_and_values, options}, _from, state) do
    protocol(:replace, [keys_and_values], options, state)
  end

  def handle_call({:get, keys, options}, _from, state) do
    protocol(:get, [keys], options, state)
  end

  def handle_call({:delete, keys, options}, _from, state) do
    protocol(:delete, [keys], options, state)
  end

  def handle_call({:flush, options}, _from, state) do
    protocol(:flush, [], options, state)
  end

  defp establish_connection(server) do
    [host, port] = String.split(server, ":")

    host = String.to_charlist(host)
    port = String.to_integer(port)

    :gen_tcp.connect(host, port, [:binary, active: false])
  end

  defp protocol(func_name, args, options, state) do
    options = Keyword.merge(state.options, options)
    args = [state.socket] ++ args ++ [options]
    {:reply, apply(state.protocol, func_name, args), state}
  end

end
