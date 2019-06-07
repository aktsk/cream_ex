defmodule Cream.Connection do
  @type config :: Keyword.t
  @type reason :: String.t
  @type key :: String.t
  @type value :: binary
  @type keys_and_values :: %{required(key) => value} | [{key, value}]

  defmacro __using__(opts) do
    quote location: :keep, bind_quoted: [opts: opts] do

      @module Cream.Connection
      @otp_app opts[:otp_app]

      def init(config), do: {:ok, config}
      defoverridable [init: 1]

      def start_link(config \\ []) do
        mix_config = Application.get_env(@otp_app, __MODULE__)
        with {:ok, mix_config} <- init(mix_config) do
          Keyword.merge(mix_config, config) |> @module.start_link(name: __MODULE__)
        end
      end

      def defaults, do: @module.defaults(__MODULE__)

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

  def start_link(options \\ []) do
    Cream.Worker.Connection.start_link(options)
  end

  def defaults(pid) do
    call(pid, :defaults)
  end

  def set(pid, keys_and_values, options \\ []) do
    call(pid, {:set, keys_and_values, options})
  end

  def set!(pid, keys_and_values, options \\ []) do
    set(pid, keys_and_values, options) |> bang
  end

  def add(pid, keys_and_values, options \\ []) do
    call(pid, {:add, keys_and_values, options})
  end

  def add!(pid, keys_and_values, options \\ []) do
    add(pid, keys_and_values, options) |> bang
  end

  def replace(pid, keys_and_values, options \\ []) do
    call(pid, {:replace, keys_and_values, options})
  end

  def replace!(pid, keys_and_values, options \\ []) do
    replace(pid, keys_and_values, options) |> bang
  end

  def get(pid, keys, options \\ []) do
    call(pid, {:get, keys, options})
  end

  def get!(pid, keys, options \\ []) do
    get(pid, keys, options) |> bang
  end

  def delete(pid, keys, options \\ []) do
    call(pid, {:delete, keys, options})
  end

  def delete!(pid, keys, options \\ []) do
    delete(pid, keys, options) |> bang
  end

  def flush(pid, options \\ []) do
    call(pid, {:flush, options})
  end

  defp bang({:ok, values}), do: values
  defp bang({:error, reasons}), do: raise(reasons)

  defp call(pid, arg), do: GenServer.call(pid, arg)

end
