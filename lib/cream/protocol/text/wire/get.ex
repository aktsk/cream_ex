defmodule Cream.Protocol.Text.Wire.Get do

  def send(conn, keys) do
    keys = Enum.join(keys, " ")
    Cream.Connection.send(conn, "get #{keys}\r\n")
  end

  def recv(conn) do
    recv(conn, [])
  end

  defp recv(conn, acc) do
    with {:ok, line} <- Cream.Connection.recv(conn, :line),
      {:ok, size} <- parse_size(line),
      {:ok, value} <- Cream.Connection.recv(conn, size + 2) # Plus two for the \r\n
    do
      item = {line, value}
      recv(conn, [item | acc])
    else
      :end -> {:ok, Enum.reverse(acc)}
      error -> error
    end
  end

  defp parse_size("END\r\n"), do: :end
  defp parse_size(line) do
    ["VALUE", _key, _flags, size] = String.split(line)
    {:ok, String.to_integer(size)}
  end

end
