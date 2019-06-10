defmodule Cream.Protocol.Text.Get do

  alias Cream.Connection

  def call(conn, key) when is_binary(key) do
    call(conn, [key]) |> List.first
  end

  def call(conn, keys) when is_list(keys) do
    command = "get " <> Enum.join(keys, " ") <> "\r\n"
    with Connection.send(conn, command) do
      keys_and_values(conn)
    end
  end

  defp keys_and_values(conn, acc \\ []) do
    with {:ok, line} <- Connection.recv(conn, :line),
      {key, flags, bytes, cas_unique} <- parse_value_line(line),
      {:ok, value} <- Connection.recv(conn, bytes + 2) # Gotta plus 2 for the \r\n
    do
      value = String.replace_suffix(value, "\r\n", "")
      acc = [{key, value, flags, cas_unique} | acc]
      keys_and_values(conn, acc)
    else
      :done -> acc
    end
  end

  defp parse_value_line("END\r\n"), do: :done
  defp parse_value_line(<<"VALUE", rest::binary>>) do
    {key, flags, bytes, cas_unique} = case String.split(rest) do
      [key, flags, bytes] -> {key, flags, bytes, nil}
      [key, flags, bytes, cas_unique] -> {key, flags, bytes, cas_unique}
    end

    {key, String.to_integer(flags), String.to_integer(bytes), cas_unique}
  end

end
