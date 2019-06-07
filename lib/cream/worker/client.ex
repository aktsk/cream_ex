defmodule Cream.Worker.Client do
  use GenServer

  defstruct [:options, :connections, :continuum]

  alias Cream.{Connection, Continuum}

  def start_link(options, gs_options) do
    GenServer.start_link(__MODULE__, options, gs_options)
  end

  def init(options) do
    connections = Map.new(options[:servers], fn server ->
      {:ok, pid} =
        options
        |> Keyword.put(:server, server)
        |> Keyword.delete(:name)
        |> Connection.start_link
      {server, pid}
    end)

    {
      :ok,
      %__MODULE__{
        options: options,
        connections: connections,
        continuum: Continuum.new(options[:servers])
      }
    }
  end

  def handle_call(:debug, _from, state) do
    {:reply, state, state}
  end

  def handle_call(:options, _from, state) do
    {:reply, state.options, state}
  end

  # If there is no continuum (only one connection), then we can just passthrough.
  def handle_call(args, _from, %{connections: connection, continuum: nil} = state) do
    {:reply, GenServer.call(connection, args), state}
  end

end
