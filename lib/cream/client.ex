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

      def set(keys_and_values, opts \\ []), do: Cream.Client.set(__MODULE__, keys_and_values, opts)
      def set!(keys_and_values, opts \\ []), do: Cream.Client.set!(__MODULE__, keys_and_values, opts)

      def add(keys_and_values, opts \\ []), do: Cream.Client.add(__MODULE__, keys_and_values, opts)
      def add!(keys_and_values, opts \\ []), do: Cream.Client.add!(__MODULE__, keys_and_values, opts)

      def replace(keys_and_values, opts \\ []), do: Cream.Client.replace(__MODULE__, keys_and_values, opts)
      def replace!(keys_and_values, opts \\ []), do: Cream.Client.replace!(__MODULE__, keys_and_values, opts)

      def flush(opts \\ []), do: Cream.Client.flush(__MODULE__, opts)

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
    pool: 5,
    ttl: 0
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

  def set(pool, keys_and_values, options \\ []) do
    call(pool, {:set, keys_and_values, options})
  end

  def set!(pool, keys_and_values, options \\ []) do
    set(pool, keys_and_values, options) |> bang
  end

  def add(pool, keys_and_values, options \\ []) do
    call(pool, {:add, keys_and_values, options})
  end

  def add!(pool, keys_and_values, options \\ []) do
    add(pool, keys_and_values, options) |> bang
  end

  def replace(pool, keys_and_values, options \\ []) do
    call(pool, {:replace, keys_and_values, options})
  end

  def replace!(pool, keys_and_values, options \\ []) do
    replace(pool, keys_and_values, options) |> bang
  end

  def get(pool, keys, options \\ []) do
    call(pool, {:get, keys, options})
  end

  def get!(pool, keys, options \\ []) do
    get(pool, keys, options) |> bang
  end

  def flush(pool, options \\ []) do
    call(pool, {:flush, options})
  end

  defp bang({:ok, values}), do: values
  defp bang({:error, reasons}), do: raise(reasons)

  defp call(pool, arg) do
    with_client(pool, &GenServer.call(&1, arg))
  end

  defp with_client(pool, f) do
    :poolboy.transaction(pool, f)
  end

end
