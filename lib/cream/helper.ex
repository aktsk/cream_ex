defmodule Cream.Helper do

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
