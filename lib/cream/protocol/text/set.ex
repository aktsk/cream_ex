defmodule Cream.Protocol.Text.Set do

  alias Cream.{Connection, Coder}

  def call(conn, key, value, options) do
    coder = options[:coder] || Coder.Raw
    ttl = options[:ttl] || 0

    {flags, value} = coder.encode(value)

    command = "set #{key} #{flags} #{ttl} #{byte_size(value)}\r\n"
    payload = "#{value}\r\n"

    with :ok <- Connection.send(conn, command),
      :ok <- Connection.send(conn, payload)
    do
      Connection.recv(conn, :line)
    end
  end

end
