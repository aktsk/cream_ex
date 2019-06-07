defmodule Cream.Utils do
  @moduledoc false

  def normalize_servers(servers) do
    List.wrap(servers) |> Enum.map(fn server ->
      case to_string(server) |> String.split(":") do
        [host | []] -> "#{host}:11211"
        [host, port | []] -> "#{host}:#{port}"
      end
    end)
  end

  def parse_server(server) do
    [host, port | []] = server |> to_string |> String.split(":")
    {host, String.to_integer(port)}
  end

  def stream_keys(keys_and_values) do
    Stream.map(keys_and_values, fn {key, _value} -> key end)
  end

  def keys(keys_and_values) do
    Enum.map(keys_and_values, fn {key, _value} -> key end)
  end

  def return_tuple(result, status), do: {status, result}

  def iolist_append(list, %{} = message) do
    alias Cream.Protocol.Binary.Message
    iolist_append(list, Message.to_iolist(message))
  end

  def iolist_append(list, item), do: [list, item]

  defmacro bytes(n) do
    bytes = n*8
    quote do: size(unquote(bytes))
  end

end
