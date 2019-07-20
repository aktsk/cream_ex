defmodule Cream.Protocol.Binary do
  @moduledoc """
  Higher level API for speaking the binary protocol over a raw connection.

  This module is for executing high level commands over a `Cream.Connection` using
  the binary protocol.

  A "high level" command is something that sends and receives multiple binary protocol packets.

  This module is still considered low level since it operates on a `Cream.Connection`. It is unaware
  of clustering, connection pooling, and the text/ascii protocol.

  You probably want `Cream.Client` instead of this.
  """

  @type conn :: Cream.Connection.t
  @type key :: String.t
  @type options :: Keyword.t

  @type reason :: String.t

  alias __MODULE__.Api

  @doc """
  Get a key.

  ## Options
  * `:cas` (boolean) - If true, returns `{value, cas}` instead of just `value`.

  ## Examples
  ```elixir
  {:ok, "bar"} = get(conn, "foo")
  {:ok, {"bar", 12345}} = get(conn, "foo", cas: true)
  ```
  """
  @spec get(conn, key, options) :: {:ok, String.t | nil} | {:error, reason}
  defdelegate get(conn, key, options \\ []), to: Api.Get, as: :call

  @doc """
  Get multiple keys.

  ## Options
  * `:cas` (boolean) - If true, return the cas value along with the values.

  ## Examples
  ```elixir
  {:ok, [{"foo", "bar"}, {"pee", "poo"}]} = mget(conn, ["foo", "pee", "gah"])
  {:ok, [{"foo", {"bar", 123}}, {"pee", {"poo", 456}}]} = mget(conn, ["foo", "pee", "gah"], cas: true)
  ```
  """
  defdelegate mget(conn, keys, options \\ []),      to: Api.Mget, as: :call
  defdelegate set(conn, key, value, options \\ []), to: Api.Set,  as: :call
  defdelegate mset(conn, enum, options \\ []),      to: Api.Mset,  as: :call

end
