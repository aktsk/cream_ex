defmodule Cream.Protocol.Binary do

  alias Cream.Protocol.Binary.Message
  alias Cream.Protocol.Reason

  def flush(socket, options) do
    extra = extra(:flush, options)

    Message.binary(:flush, extra: extra)
    |> socket_send(socket)

    with {:ok, message} <- recv_message(socket) do
      case message do
        %{status: 0} -> {:ok, :flushed}
        %{value: reason} -> {:error, Reason.tr(reason)}
      end
    end
  end

  def set(socket, {key, value}, options) do
    extra = extra(:store, options)

    Message.binary(:set, key: key, value: value, extra: extra)
    |> socket_send(socket)

    with {:ok, message} <- recv_message(socket) do
      case message do
        %{status: 0} -> {:ok, :stored}
        %{value: reason} -> {:error, Reason.tr(reason)}
      end
    end
  end

  def set(socket, keys_and_values, options) do
    extra = extra(:store, options)

    Enum.map(keys_and_values, fn {key, value} ->
      Message.binary(:set, key: key, value: value, extra: extra)
    end)
    |> append(Message.binary(:noop))
    |> socket_send(socket)

    with {:ok, messages} <- recv_messages(socket) do
      errors = keys_and_values
      |> Stream.zip(messages)
      |> Enum.reduce(%{}, fn {{key, _value}, message}, acc ->
        case message do
          %{status: 0} -> acc
          %{value: reason} -> Map.put(acc, key, Reason.tr(reason))
        end
      end)

      if errors == %{} do
        {:ok, :stored}
      else
        {:error, errors}
      end
    end
  end

  def add(socket, {key, value}, options) do
    extra = extra(:store, options)

    Message.binary(:add, key: key, value: value, extra: extra)
    |> socket_send(socket)

    with {:ok, message} <- recv_message(socket) do
      case message do
        %{status: 0} -> {:ok, :stored}
        %{value: reason} -> {:error, Reason.tr(reason)}
      end
    end
  end

  def add(socket, keys_and_values, options) do
    extra = extra(:store, options)

    Enum.map(keys_and_values, fn {key, value} ->
      Message.binary(:add, key: key, value: value, extra: extra)
    end)
    |> append(Message.binary(:noop))
    |> socket_send(socket)

    with {:ok, messages} <- recv_messages(socket) do
      errors = keys_and_values
      |> Stream.zip(messages)
      |> Enum.reduce(%{}, fn {{key, _value}, message}, acc ->
        case message do
          %{status: 0} -> acc
          %{value: reason} -> Map.put(acc, key, Reason.tr(reason))
        end
      end)

      if errors == %{} do
        {:ok, :stored}
      else
        {:error, errors}
      end
    end
  end

  def replace(socket, {key, value}, options) do
    replace(socket, [{key, value}], options)
    |> response_for(key)
  end

  def replace(socket, keys_and_values, options) do
    extra = extra(:store, options)

    Enum.map(keys_and_values, fn {key, value} ->
      Message.binary(:replace, key: key, value: value, extra: extra)
    end)
    |> append(Message.binary(:noop))
    |> socket_send(socket)

    with {:ok, messages} <- recv_messages(socket) do
      errors = keys_and_values
      |> Stream.zip(messages)
      |> Enum.reduce(%{}, fn {{key, _value}, message}, acc ->
        case message do
          %{status: 0} -> acc
          %{value: reason} ->
            reason = case Reason.tr(reason) do
              :not_found -> :not_stored
              reason -> reason
            end
            Map.put(acc, key, reason)
        end
      end)

      if errors == %{} do
        {:ok, :stored}
      else
        {:error, errors}
      end
    end
  end

  def get(socket, keys, _options) when is_list(keys) do
    Enum.map(keys, &Message.binary(:getkq, key: &1))
    |> append(Message.binary(:noop))
    |> socket_send(socket)

    with {:ok, messages} <- recv_messages(socket) do
      {:ok, Map.new(messages, &{&1.key, &1.value})}
    end
  end

  def get(socket, key, _options) do
    Message.binary(:get, key: key)
    |> socket_send(socket)

    with {:ok, message} <- recv_message(socket) do
      case message do
        %{status: 0, value: value} -> {:ok, value}
        %{status: 1} -> {:ok, nil}
        %{value: reason} -> {:error, Reason.tr(reason)}
      end
    end
  end

  defp response_for(response, key) do
    case response do
      {:error, %{^key => reason}} -> {:error, reason}
      response -> response
    end
  end

  defp append(list, item), do: [list, item]

  defp extra(:store, options) do
    flags = if options[:coder], do: 1, else: 0
    ttl = options[:ttl]

    <<
      flags :: size(32),
      ttl   :: size(32)
    >>
  end

  defp extra(:flush, options) do
    if options[:delay] do
      <<options[:delay] :: size(32)>>
    else
      <<>>
    end
  end

  defp recv_message(socket) do
    with {:ok, message} <- recv_header(socket) do
      recv_body(message, socket)
    end
  end

  def recv_header(socket) do
    with {:ok, data} <- :gen_tcp.recv(socket, 24) do
      {:ok, Message.from_binary(data)}
    end
  end

  def recv_body(%{total_body: 0} = message, _socket), do: {:ok, message}
  def recv_body(%{total_body: total_body} = message, socket) do
    with {:ok, data} <- :gen_tcp.recv(socket, total_body) do
      {:ok, Message.from_binary(message, data)}
    end
  end

  defp recv_messages(socket, messages \\ []) do
    with {:ok, message} = recv_message(socket) do
      case message do
        %{opcode: :noop} -> {:ok, Enum.reverse(messages)}
        message -> recv_messages(socket, [message | messages])
      end
    end
  end

  defp socket_send(data, socket) do
    :ok = :gen_tcp.send(socket, data)
  end

end
