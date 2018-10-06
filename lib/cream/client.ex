defmodule Cream.Client do
  @moduledoc """
  Client for memcached's ascii/text protocol.

  This is meant to be used with a reverse proxy, thus you cannot connect to multiple
  memcached servers with this client.

  Recommended reverse proxies:
   * [mcrouter](https://github.com/facebook/mcrouter)
   * [twemproxy](https://github.com/twitter/twemproxy)
   * [dynomite](https://github.com/Netflix/dynomite)

  ## Basic usage

  ```elixir
  {:ok, client} = Cream.Client.start_link # Use defaults
  {:ok, :stored} = Cream.Client.set(client, "name", "Callie")
  {:ok, "Callie"} = Cream.Client.get(client, "name")
  ```

  ## Module usage

  ```elixir
  defmodule YourClient do
   use Cream.Client, otp_app: :your_app
  end

  use Mix.Config
  config :your_app, YourClient, [] # Use defaults

  YourClient.start_link
  {:ok, :stored} = YourClient.set("name", "Callie")
  {:ok, "Callie"} = YourClient.get("name")
  ```

  ## Configuration

  For all configuration options, see `t:config/0`.

  When using a module, you can do compile time configuration via `Mix.Config`.
  ```elixir
  use Mix.Config
  config :your_app, YourClient,
    server: "memcached-proxy.company.com",
    pool: 10
  ```

  And/or you can do runtime configuration via the `c:init/1` callback.
  ```
  defmodule YourClient do
    use Cream.Client, otp_app: :your_app

    def init(config) do
      config = config
      |> Keyword.put(:server, "memcached-proxy.company.com")
      |> Keyword.put(:pool, 10)
      {:ok, config}
    end
  end
  ```
  """

  @typedoc """
  Configuration options.

  Defaults:
  ```elixir
  [
    server: "localhost:11211",
    pool: 5,
    ttl: 0,
    namespace: nil
  ]
  ```
  """
  @type config :: Keyword.t
  @typedoc """
  Error reason.

  It's a string. Pretty easy.
  """
  @type reason :: String.t

  @type key :: String.t

  @type value :: binary

  @type keys_and_values :: %{required(key) => value} | [{key, value}]

  defmacro __using__(opts) do
    quote location: :keep, bind_quoted: [opts: opts] do

      @otp_app opts[:otp_app]

      def init(config), do: {:ok, config}
      defoverridable [init: 1]

      def start_link(config \\ []) do
        Cream.Client.start_link(__MODULE__, @otp_app, config)
      end

      def get(key_or_keys, opts \\ []), do: Cream.Client.get(__MODULE__, key_or_keys, opts)
      def get!(key_or_keys, opts \\ []), do: Cream.Client.get!(__MODULE__, key_or_keys, opts)
      def gets(key_or_keys, opts \\ []), do: Cream.Client.gets(__MODULE__, key_or_keys, opts)
      def gets!(key_or_keys, opts \\ []), do: Cream.Client.gets!(__MODULE__, key_or_keys, opts)

      def set(keys_and_values, opts \\ []), do: Cream.Client.set(__MODULE__, keys_and_values, opts)
      def set!(keys_and_values, opts \\ []), do: Cream.Client.set!(__MODULE__, keys_and_values, opts)

      def add(key, value, opts \\ []), do: Cream.Client.add(__MODULE__, key, value, opts)
      def replace(key, value, opts \\ []), do: Cream.Client.replace(__MODULE__, key, value, opts)
      def cas(key, value, cas, opts \\ []), do: Cream.Client.cas(__MODULE__, key, value, cas, opts)

      def delete(key), do: Cream.Client.delete(__MODULE__, key)

      def flush(opts \\ []), do: Cream.Client.flush(__MODULE__, opts)

      def mset(keys_and_values, opts \\ []), do: Cream.Client.mset(__MODULE__, keys_and_values, opts)

    end
  end

  @doc """
  Callback for runtime/dynamic configuration.

  ```
  defmodule YourClient do
    use Cream.Client, otp_app: :your_app

    def init(config) do
      config = config
      |> Keyword.put(:server, "memcached-proxy.company.com")
      |> Keyword.put(:pool, 10)
      {:ok, config}
    end
  end
  ```
  """
  @callback init(config) :: {:ok, config} | {:error, reason}

  @defaults [
    server: "localhost:11211",
    pool: 5
  ]
  def start_link(opts \\ [], gen_opts \\ []) do
    opts = Keyword.merge(@defaults, opts)

    poolboy_config = [
      worker_module: Cream.Worker.Client,
      size: opts[:pool]
    ]

    poolboy_config = if gen_opts[:name] do
      Keyword.put(poolboy_config, :name, {:local, gen_opts[:name]})
    else
      poolboy_config
    end

    :poolboy.start_link(poolboy_config, opts)
  end

  @doc false
  def start_link(mod, otp_app, opts) do
    config = Application.get_env(otp_app, mod)
    with {:ok, config} <- mod.init(config) do
      Keyword.merge(config, opts) |> start_link(name: mod)
    end
  end

  # def get(pool, key_or_keys, options \\ [])
  #
  # def get(pool, keys, options) when is_list(keys) do
  #   with_client(pool, &GenServer.call(&1, {:get, keys, options}))
  # end
  #
  # def get(pool, key, options) when not is_list(key) do
  #   pool
  #   |> get([key], options)
  #   |> extract_single_value(key)
  # end
  #
  # def get!(pool, key_or_keys, options) do
  #   case get(pool, key_or_keys, options) do
  #     {:ok, value_or_values} -> value_or_values
  #     {:error, reason} -> raise(reason)
  #   end
  # end
  #
  # def gets(pool, key_or_keys, options \\ [])
  #
  # def gets(pool, keys, options) when is_list(keys) do
  #   with_client(pool, &GenServer.call(&1, {:gets, keys, options}))
  # end
  #
  # def gets(pool, key, options) when not is_list(key) do
  #   pool
  #   |> gets([key], options)
  #   |> extract_single_value(key)
  # end
  #
  # def gets!(pool, key_or_keys, options) do
  #   case gets(pool, key_or_keys, options) do
  #     {:ok, value_or_values} -> value_or_values
  #     {:error, reason} -> raise(reason)
  #   end
  # end

  @defaults [
    ttl: 0
  ]

  def set(pool, keys_and_values, options \\ []) do
    call(pool, {:set, keys_and_values, options})
  end

  def set!(pool, keys_and_values, options \\ []) do
    case set(pool, keys_and_values, options) do
      {:ok, :stored} -> :stored
      {:error, reason} -> raise(reason)
      responses -> Map.new(responses, fn {key, response} ->
        case response do
          {:ok, :stored} -> {key, :stored}
          {:error, reason} -> raise(reason)
        end
      end)
    end
  end

  def get(pool, keys, options \\ []) do
    call(pool, {:get, keys, options})
  end

  def get!(pool, keys, options \\ []) do
    case get(pool, keys, options) do
      {:ok, value} -> value
      {:error, reason} -> raise(reason)
      responses -> Enum.reduce(responses, %{}, fn {key, response}, acc ->
        case response do
          {:ok, nil} -> acc
          {:ok, value} -> Map.put(acc, key, value)
          {:error, reason} -> raise(reason)
        end
      end)
    end
  end

  def add(pool, key, value, options \\ []) do
    store("add", pool, {key, value}, options)
  end

  def replace(pool, key, value, options \\ []) do
    store("replace", pool, {key, value}, options)
  end

  def cas(pool, key, value, cas, options \\ []) do
    options = Keyword.put(options, :cas, cas)
    store("cas", pool, {key, value}, options)
  end

  def delete(pool, key) do
    with_client(pool, &GenServer.call(&1, {:delete, key}))
  end

  def flush(pool, options \\ []) do
    call(pool, {:flush, options})
  end

  @defaults [
    ttl: 0
  ]
  defp store(cmd, pool, key_value, options) do
    options = Keyword.merge(@defaults, options)
    with_client(pool, &GenServer.call(&1, {:store, cmd, key_value, options}))
  end

  defp extract_single_value(result, key) do
    case result do
      {:ok, map} when map == %{} -> {:ok, nil}
      {:ok, %{^key => value}} -> {:ok, value}
      error -> error
    end
  end

  defp call(pool, arg) do
    with_client(pool, &GenServer.call(&1, arg))
  end

  defp with_client(pool, f) do
    :poolboy.transaction(pool, f)
  end

end
