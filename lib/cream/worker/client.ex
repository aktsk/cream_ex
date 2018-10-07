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

  def handle_call({:add, keys_and_values, options}, _from, state) do
    protocol(:add, [keys_and_values], options, state)
  end

  def handle_call({:replace, keys_and_values, options}, _from, state) do
    protocol(:replace, [keys_and_values], options, state)
  end

  def handle_call({:get, keys, options}, _from, state) do
    protocol(:get, [keys], options, state)
  end

  def handle_call({:flush, options}, _from, state) do
    protocol(:flush, [], options, state)
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
