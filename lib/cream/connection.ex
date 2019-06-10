defmodule Cream.Connection do
  defstruct [:host, :port, :protocol, :socket]
  use Connection

  @defaults [
    host: "localhost",
    port: 11211,
    protocol: :ascii
  ]

  def start_link(args \\ [], opts \\ []) do
    Connection.start_link(__MODULE__, Keyword.merge(args, @defaults), opts)
  end

  def send(conn, data) do
    Connection.call(conn, {:send, data})
  end

  def recv(conn, bytes, timeout \\ 1000) do
    Connection.call(conn, {:recv, bytes, timeout})
  end

  def init(args) do
    {:connect, :init, struct!(__MODULE__, args)}
  end

  def connect(_info, state) do
    case Socket.connect("tcp://#{state.host}:#{state.port}") do
      {:ok, socket} -> {:ok, %{state | socket: socket}}
    end
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

  def handle_call({:recv, bytes, timeout}, _from, state) do
    bytes = normalize_bytes(bytes, state.socket)

    case Socket.Stream.recv(state.socket, bytes, timeout: timeout) do
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
