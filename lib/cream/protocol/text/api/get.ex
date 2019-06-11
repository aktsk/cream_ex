defmodule Cream.Protocol.Text.Api.Get do

  def call(conn, key, options \\ []) do

    wire = if options[:cas] do
      Cream.Protocol.Text.Wire.Gets
    else
      Cream.Protocol.Text.Wire.Get
    end

    with :ok <- wire.send(conn, [key]),
      {:ok, [item]} <- wire.recv(conn)
    do
      {line, value} = item
      value = String.replace_suffix(value, "\r\n", "")

      if options[:cas] do
        ["VALUE", _key, _flags, _size, cas] = String.split(line)
        {cas, value}
      else
        value
      end
    else
      {:ok, []} -> nil
      error -> error
    end
  end

end
