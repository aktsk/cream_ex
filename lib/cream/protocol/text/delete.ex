defmodule Cream.Protocol.Text.Delete do

  alias Cream.Connection

  def call(conn, key, options) do
    noreply = if options[:noreply], do: "noreply", else: nil
    command = "delete #{key} #{noreply}\r\n"

    with :ok <- Connection.send(conn, command) do
      if options[:noreply] do
        nil
      else
        Connection.recv(conn, :line)
      end
    end
  end

end
