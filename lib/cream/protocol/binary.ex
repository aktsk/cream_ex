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

    case recv_message(socket) do
      %{status: 0} -> {:ok, :stored}
      %{value: reason} -> {:error, reason}
    end
  end

  def set(socket, keys_and_values, options) do
    extra = extra(:set, options)

    Enum.reduce(keys_and_values, [new_message(:noop)], fn {key, value}, acc ->
      [new_message(:setq, key: key, value: value, extra: extra) | acc]
    end)
    |> send_messages(socket)

    case recv_messages(socket) do
      [] -> {:ok, :stored}
      messages -> {:error, Map.new(messages, fn message ->
        {message.key, message.value}
      end)}
    end
  end

  def get(socket, keys, _options) when is_list(keys) do
    Enum.reduce(keys, [new_message(:noop)], fn key, acc ->
      [new_message(:getkq, key: key) | acc]
    end)
    |> to_binary
    |> socket_send(socket)

    case recv_messages(socket) do
      {:error, _reason} = error -> error
      messages -> {:ok, Map.new(messages, &{&1.key, &1.value})}
    end
  end

  def get(socket, key, _options) do
    new_message(:get, key: key)
    |> send_message(socket)

    case recv_message(socket) do
      %{status: 0, value: value} -> {:ok, value}
      %{status: 1} -> {:ok, nil}
      %{value: reason} -> {:error, reason}
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
    recv_header(socket)
    |> recv_body(socket)
  end

  def recv_header(socket) do
    {:ok, data} = :gen_tcp.recv(socket, 24)
    Message.from_binary(data)
  end

  def recv_body(%{total_body: 0} = message, _socket), do: message
  def recv_body(%{total_body: total_body} = message, socket) do
    {:ok, data} = :gen_tcp.recv(socket, total_body)
    Message.from_binary(message, data)
  end

  defp recv_messages(socket, messages \\ []) do
    case recv_message(socket) do
      %{opcode: :noop} -> Enum.reverse(messages)
      message -> recv_messages(socket, [message | messages])
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
