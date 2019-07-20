defmodule Cream.Connection do
  defstruct [:server, :socket]
  use Connection

  require Logger

  @defaults [
    host: "localhost",
    port: 11211
  ]

  def start_link(args \\ [], opts \\ []) do
    Connection.start_link(__MODULE__, Keyword.merge(@defaults, args), opts)
  end

  def send(conn, data) do
    Connection.call(conn, {:send, data})
  end

  def recv(conn, bytes, timeout \\ 1000) do
    Connection.call(conn, {:recv, bytes, timeout})
  end

  def init(args) do
    server = case Map.new(args) do
      %{server: server} -> server
      %{host: host, port: port} -> "#{host}:#{port}"
    end
    {:connect, :init, %__MODULE__{server: server}}
  end

  def connect(_info, state) do
    case Socket.connect("tcp://#{state.server}") do
      {:ok, socket} ->
        Logger.debug "Connection established to #{state.server}"
        state = %{state | socket: socket}
        {:ok, state}
      {:error, reason} ->
        reason = :inet.format_error(reason)
        Logger.warn "Connection failed to #{state.server} (#{reason})"
        {:backoff, 1000, state}
    end
  end

  def disconnect(info, state) do
    :ok = Socket.close(state.socket)
    state = %{state | socket: nil}

    case info do
      {:error, reason} ->
        reason = :inet.format_error(reason)
        Logger.warn("Disconnected #{state.server} (#{reason})")
    end

    {:connect, :reconnect, state}
  end

  def handle_call(_, _, %{socket: nil} = state) do
    {:reply, {:error, :closed}, state}
  end

  def handle_call({:send, data}, _from, state) do
    case Socket.Stream.send(state.socket, data) do
      :ok -> {:reply, :ok, state}
      {:error, _reason} = info -> {:disconnect, info, info, state}
    end
  end

  def handle_call({:recv, bytes, timeout}, from, state) do
    bytes = normalize_bytes(bytes, state.socket)

    case Socket.Stream.recv(state.socket, bytes, timeout: timeout) do
      {:ok, nil} -> handle_call({:recv, bytes, timeout}, from, state)
      {:ok, _data} = ok -> {:reply, ok, state}
      {:error, :timeout} = timeout -> {:reply, timeout, state}
      {:error, _reason} = error -> {:disconnect, error, error, state}
    end
  end

  defp normalize_bytes(bytes, socket) do
    case bytes do
      :line ->
        Socket.packet!(socket, :line)
        0
      size ->
        Socket.packet!(socket, :raw)
        size
    end
  end

end
