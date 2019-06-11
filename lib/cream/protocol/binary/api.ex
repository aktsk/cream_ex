defmodule Cream.Protocol.Binary.Api do

  defdelegate get(conn, key), to: __MODULE__.Get, as: :call
  defdelegate set(conn, key, value, options \\ []), to: __MODULE__.Set, as: :call

end
