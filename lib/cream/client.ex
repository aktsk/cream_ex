defmodule Cream.Client do

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

      def set(key, value, opts \\ []), do: Cream.Client.set(__MODULE__, key, value, opts)
      def add(key, value, opts \\ []), do: Cream.Client.add(__MODULE__, key, value, opts)
      def replace(key, value, opts \\ []), do: Cream.Client.replace(__MODULE__, key, value, opts)
      def cas(key, value, cas, opts \\ []), do: Cream.Client.cas(__MODULE__, key, value, cas, opts)

      def delete(key), do: Cream.Client.delete(__MODULE__, key)

      def flush_all(delay \\ nil), do: Cream.Client.flush_all(__MODULE__, delay)

    end
  end

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

  def start_link(mod, otp_app, opts) do
    config = Application.get_env(otp_app, mod)
    with {:ok, config} <- mod.init(config) do
      Keyword.merge(config, opts) |> start_link(name: mod)
    end
  end

  def get(pool, key_or_keys, options \\ [])

  def get(pool, keys, options) when is_list(keys) do
    with_client(pool, &GenServer.call(&1, {:get, keys, options}))
  end

  def get(pool, key, options) when not is_list(key) do
    pool
    |> get([key], options)
    |> extract_single_value(key)
  end

  def get!(pool, key_or_keys, options \\ []) do
    case get(pool, key_or_keys, options) do
      {:ok, value_or_values} -> value_or_values
      {:error, reason} -> raise(reason)
    end
  end

  def gets(pool, key_or_keys, options \\ [])

  def gets(pool, keys, options) when is_list(keys) do
    with_client(pool, &GenServer.call(&1, {:gets, keys, options}))
  end

  def gets(pool, key, options) when not is_list(key) do
    pool
    |> gets([key], options)
    |> extract_single_value(key)
  end

  def gets!(pool, key_or_keys, options \\ []) do
    case gets(pool, key_or_keys, options) do
      {:ok, value_or_values} -> value_or_values
      {:error, reason} -> raise(reason)
    end
  end

  def set(pool, key, value, options \\ []) do
    store("set", pool, key, value, options)
  end

  def add(pool, key, value, options \\ []) do
    store("add", pool, key, value, options)
  end

  def replace(pool, key, value, options \\ []) do
    store("replace", pool, key, value, options)
  end

  def cas(pool, key, value, cas, options \\ []) do
    options = Keyword.put(options, :cas, cas)
    store("cas", pool, key, value, options)
  end

  def delete(pool, key) do
    with_client(pool, &GenServer.call(&1, {:delete, key}))
  end

  def flush_all(pool, delay \\ nil) do
    with_client(pool, &GenServer.call(&1, {:flush_all, delay}))
  end

  @defaults [
    ttl: 0
  ]
  defp store(cmd, pool, key, value, options) do
    options = Keyword.merge(@defaults, options)
    with_client(pool, &GenServer.call(&1, {:store, cmd, key, value, options}))
  end

  defp extract_single_value(result, key) do
    case result do
      {:ok, map} when map == %{} -> {:ok, nil}
      {:ok, %{^key => value}} -> {:ok, value}
      error -> error
    end
  end

  defp with_client(pool, f) do
    :poolboy.transaction(pool, f)
  end

end
