defmodule Cream.Protocol.Text.FlushAll do

  alias Cream.Connection

  def call(conn, options) do
    delay = options[:delay]
    noreply = if options[:noreply], do: "noreply", else: nil
    command = "flush_all #{delay} #{noreply}\r\n"

    with :ok <- Connection.send(conn, command) do
      if options[:noreply] do
        nil
      else
        Connection.recv(conn, :line)
      end
    end
  end

end
