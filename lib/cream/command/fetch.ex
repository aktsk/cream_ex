defmodule Cream.Command.Fetch do
  use Cream.Command

  import Cream.Command.{Get, Set}
  import Cream.Utils, only: [normalize_key: 1]

  def fetch(pool, key, func) when is_atom(pool) or is_pid(pool) do
    ConnectionPool.with pool, fn cluster ->
      fetch(cluster, key, func)
    end
  end

  @spec fetch(any, key :: String.t, func :: ( -> String.t)) :: String.t
  def fetch(cluster = %Cluster{}, key, func) when is_binary(key) do
    map = fetch(cluster, [key], fn missing_keys ->
      %{ List.first(missing_keys) => func.() }
    end)
    Map.values(map) |> List.first
  end

  @spec fetch(any, keys :: [String.t], func :: ([String.t] -> %{String.t => String.t})) :: %{String.t => String.t}
  def fetch(cluster = %Cluster{}, keys, func) when is_list(keys) do
    keys = Enum.map(keys, &normalize_key/1) |> Enum.uniq
    hits = get(cluster, keys)
    missing_keys = Enum.filter(keys, &(!Map.has_key?(hits, &1)))

    total_count = length(keys)
    hit_count = Map.size(hits)
    miss_count = length(missing_keys)
    percent = if hit_count == 0, do: 0, else: round(total_count / hit_count * 100)
    Logger.debug "[cream] fetch - hits:#{hit_count} misses:#{miss_count} efficiency:#{percent}%"

    if !Enum.empty?(missing_keys) do
      fetched = func.(missing_keys) |> to_map
      set(cluster, fetched)
      Map.merge(hits, fetched)
    else
      hits
    end
  end

  defp to_map(m) when is_map(m), do: m
  defp to_map(m), do: Enum.into(m, %{})

end