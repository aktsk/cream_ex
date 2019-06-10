defmodule Cream.Protocol.Text.Stats do

  alias Cream.Connection

  def call(conn, arg) do
    command = "stats #{arg}\r\n"

    with :ok <- Connection.send(conn, command) do
      collect_stats(conn)
    end
  end

  defp collect_stats(conn, acc \\ []) do
    with {:ok, line} <- Connection.recv(conn, :line),
      [name, value] <- parse_stat_line(line)
    do
      collect_stats(conn, [{name, value} | acc])
    else
      :done -> acc
    end
  end

  defp parse_stat_line("END\r\n"), do: :done
  defp parse_stat_line(<<"STAT", rest::binary>>) do
    String.split(rest)
  end

end
