defmodule Cream.Protocol.Binary do

  defdelegate get(conn, key), to: __MODULE__.Get, as: :call

end
