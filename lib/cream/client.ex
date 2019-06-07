defmodule Cream.Client do

  @type client :: GenServer.t
  @type key :: String.t
  @type value :: binary
  @type keys_and_values :: [{key, value}]
  @type options :: Keyword.t
  @type storage_result :: :stored | :not_stored | :exists | :not_found
  @type storage_results :: %{
    optional(storage_result) => [key],
    optional(:error) => %{
      reason => [key]
    }
  }
  @type reason :: String.t

  defmacro __using__(opts) do
    quote location: :keep, bind_quoted: [opts: opts] do

      @module Cream.Client
      @otp_app opts[:otp_app]

      def init(config), do: {:ok, config}
      defoverridable [init: 1]

      def start_link(options \\ []), do: @module.start_link(__MODULE__, @otp_app, options)

      def debug, do: @module.debug(__MODULE__)
      def options, do: @module.options(__MODULE__)

      def get(key_or_keys, opts \\ []), do: @module.get(__MODULE__, key_or_keys, opts)
      def get!(key_or_keys, opts \\ []), do: @module.get!(__MODULE__, key_or_keys, opts)

      def set(keys_and_values, opts \\ []), do: @module.set(__MODULE__, keys_and_values, opts)
      def set!(keys_and_values, opts \\ []), do: @module.set!(__MODULE__, keys_and_values, opts)

      def add(keys_and_values, opts \\ []), do: @module.add(__MODULE__, keys_and_values, opts)
      def add!(keys_and_values, opts \\ []), do: @module.add!(__MODULE__, keys_and_values, opts)

      def replace(keys_and_values, opts \\ []), do: @module.replace(__MODULE__, keys_and_values, opts)
      def replace!(keys_and_values, opts \\ []), do: @module.replace!(__MODULE__, keys_and_values, opts)

      def delete(keys, opts \\[]), do: @module.delete(__MODULE__, keys, opts)
      def delete!(keys, opts \\[]), do: @module.delete!(__MODULE__, keys, opts)

      def flush(opts \\ []), do: @module.flush(__MODULE__, opts)

    end
  end

  @defaults [
    server: "localhost:11211",
    pool: 5,
    ttl: 0,
    protocol: :binary
  ]

  def start_link(options \\ []) do
    {options, gs_options} =
      @defaults
      |> Keyword.merge(options)
      |> Enum.split_with(fn {key, _value} ->
        Keyword.has_key?(@defaults, key)
      end)

    if options[:servers] do
      Cream.Worker.Client.start_link(options, gs_options)
    else
      Cream.Worker.Connection.start_link(options, gs_options)
    end
  end

  def start_link(module, otp_app, options) do
    config = Application.get_env(otp_app, module)
    with {:ok, config} <- module.init(config) do
      Keyword.merge(config, options)
      |> Keyword.put(:name, module)
      |> start_link
    end
  end

  def debug(client) do
    GenServer.call(client, :debug)
  end

  def options(client) do
    GenServer.call(client, :options)
  end

  @spec set(client, {key, value}, options) :: {:ok, storage_result} | {:error, reason}
  @spec set(client, keys_and_values, options) :: {:ok, storage_results} | {:error, storage_results}
  def set(client, keys_and_values, options \\ []) do
    store(client, :set, keys_and_values, options)
  end

  def add(client, keys_and_values, options \\ []) do
    store(client, :add, keys_and_values, options)
  end

  def get(client, keys, options \\ []) do
    retrieve(client, :get, keys, options)
  end

  def delete(client, keys, options \\ []) do
    GenServer.call(client, {:delete, keys, options})
  end

  def flush(client, options \\ []) do
    GenServer.call(client, {:flush, options})
  end

  defp retrieve(client, cmd, key, options) when is_binary(key) do
    case retrieve(client, cmd, [key], options) do
      {status, %{^key => value}} -> {status, value}
      {status, %{}} -> {status, nil}
    end
  end

  defp retrieve(client, cmd, keys, options) when is_list(keys) do
    GenServer.call(client, {cmd, keys, options})
  end

  defp store(client, cmd, {key, value}, options) do
    {status, results} = store(client, cmd, [{key, value}], options)
    {status, results |> Map.keys |> List.first}
  end

  defp store(client, cmd, keys_and_values, options) do
    GenServer.call(client, {cmd, keys_and_values, options})
  end

end
