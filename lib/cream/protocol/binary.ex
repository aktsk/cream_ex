defmodule Cream.Protocol.Binary do

  alias Cream.Protocol.Binary.Message

  def flush(socket, options) do
    extra = extra(:flush, options)

    new_message(:flush, extra: extra)
    |> send_message(socket)

    recv_message(socket)
  end

  def set(socket, {key, value}, options) do
    extra = extra(:set, options)

    new_message(:set, key: key, value: value, extra: extra)
    |> send_message(socket)

    with {:ok, message} <- recv_message(socket) do
      case message do
        %{status: 0} -> {:ok, :stored}
        %{value: reason} -> {:error, reason}
      end
    end
  end

  def set(socket, keys_and_values, options) do
    extra = extra(:set, options)

    Enum.reduce(keys_and_values, [new_message(:noop)], fn {key, value}, acc ->
      [new_message(:set, key: key, value: value, extra: extra) | acc]
    end)
    |> send_messages(socket)

    with {:ok, messages} <- recv_messages(socket) do
      errors = keys_and_values
      |> Stream.zip(messages)
      |> Enum.reduce(%{}, fn {{key, _value}, message}, acc ->
        case message do
          %{status: 0} -> acc
          %{value: reason} -> Map.put(acc, key, reason)
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
    Enum.reduce(keys, [new_message(:noop)], fn key, acc ->
      [new_message(:getkq, key: key) | acc]
    end)
    |> to_binary
    |> socket_send(socket)

    with {:ok, messages} <- recv_messages(socket) do
      {:ok, Map.new(messages, &{&1.key, &1.value})}
    end
  end

  def get(socket, key, _options) do
    new_message(:get, key: key)
    |> send_message(socket)

    with {:ok, message} <- recv_message(socket) do
      case message do
        %{status: 0, value: value} -> {:ok, value}
        %{status: 1} -> {:ok, nil}
        %{value: reason} -> {:error, reason}
      end
    end
  end

  defp extra(:set, options) do
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

  defp new_message(opcode, fields \\ []) do
    Message.new(opcode, fields)
  end

  defp to_binary(messages) when is_list(messages) do
    Enum.map(messages, &Message.to_binary/1)
  end

  defp to_binary(message) do
    Message.to_binary(message)
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

  defp send_message(message, socket) do
    message
    |> to_binary
    |> socket_send(socket)
  end

  defp send_messages(messages, socket) do
    messages
    |> to_binary
    |> socket_send(socket)
  end

  defp socket_send(data, socket) do
    :ok = :gen_tcp.send(socket, data)
  end

end
