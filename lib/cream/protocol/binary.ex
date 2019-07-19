defmodule Cream.Protocol.Binary do

  alias __MODULE__.Api

  defdelegate get(conn, key, options \\ []),        to: Api.Get,  as: :call
  defdelegate mget(conn, keys, options \\ []),      to: Api.Mget, as: :call
  defdelegate set(conn, key, value, options \\ []), to: Api.Set,  as: :call
  defdelegate mset(conn, enum, options \\ []),      to: Api.Mset,  as: :call

end
