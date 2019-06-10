defmodule Cream.Protocol.Text do

  defdelegate set(conn, key, value, options \\ []), to: __MODULE__.Set, as: :call
  defdelegate get(conn, keys), to: __MODULE__.Get, as: :call
  defdelegate delete(conn, key, options \\ []), to: __MODULE__.Delete, as: :call
  defdelegate flush_all(conn, options \\ []), to: __MODULE__.FlushAll, as: :call
  defdelegate stats(conn, arg \\ nil), to: __MODULE__.Stats, as: :call

end
