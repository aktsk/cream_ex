defmodule Cream.Protocol.Binary do

  alias Cream.Protocol.Binary.{Message, Error}
  alias Cream.{Coder, Utils}

  import Cream.Utils, only: [iolist_append: 2]

  def set(socket, keys_and_values, options) do
    storage_commands(:set, keys_and_values, options)
    |> socket_send(socket)

    keys_and_values
    |> Utils.stream_keys
    |> keyed_response(socket, :stored)
  end

  def add(socket, keys_and_values, options) do
    storage_commands(:add, keys_and_values, options)
    |> socket_send(socket)

    keys_and_values
    |> Utils.stream_keys
    |> keyed_response(socket, :stored)
  end

  def replace(socket, keys_and_values, options) do
    storage_commands(:replace, keys_and_values, options)
    |> socket_send(socket)

    keys_and_values
    |> Utils.stream_keys
    |> keyed_response(socket, :stored)
  end

  def get(socket, keys, options) do
    Enum.map(keys, &Message.iolist(:getkq, key: &1))
    |> iolist_append(Message.iolist(:noop))
    |> socket_send(socket)

    with {:ok, messages} <- recv_messages(socket) do
      Map.new(messages, fn message ->
        value = decode_and_cas(message, options)
        {message.key, value}
      end)
      |> Utils.return_tuple(:ok)
    else
      {:error, reason} -> keyed_error(keys, reason)
    end
  end

  def delete(socket, keys, _options) do
    Enum.map(keys, &Message.iolist(:delete, key: &1))
    |> iolist_append(Message.new(:noop))
    |> socket_send(socket)

    keyed_response(keys, socket, :deleted)
  end

  def flush(socket, options) do
    Message.iolist(:flush, extras: [ttl: options[:ttl]])
    |> socket_send(socket)

    with {:ok, message} <- recv_message(socket) do
      case message do
        %{status: 0} -> {:ok, :flushed}
        %{status: status} -> {:error, Error.to_atom(status)}
      end
    end
  end

  defp storage_commands(opcode, keys_and_values, options) do
    Enum.map(keys_and_values, fn {key, value} ->
      {flags, value} = Coder.encode(options[:coder], value)
      extras = [flags: flags, ttl: options[:ttl]]
      Message.iolist(opcode, key: key, value: value, extras: extras)
    end)
    |> iolist_append(Message.new(:noop))
  end

  defp keyed_response(keys, socket, success_status) do
    with {:ok, messages} <- recv_messages(socket) do
      Stream.zip(keys, messages)
      |> Enum.reduce(%{}, fn {key, message}, acc ->
        status = Error.to_atom(message.status) || success_status
        Map.update(acc, status, [key], fn list -> [key | list] end)
      end)
      |> Utils.return_tuple(:ok)
    else
      {:error, reason} -> keyed_error(keys, reason)
    end
  end

  defp keyed_error(keys, reason) when is_list(keys) do
    {:error, %{errors: %{reason => keys}}}
  end

  # keys could be a stream.
  defp keyed_error(keys, reason) do
    keyed_error(Enum.into(keys, []), reason)
  end

  defp decode_and_cas(message, options) do
    value = Coder.decode(options[:coder], message.extras[:flags], message.value)
    if options[:cas] do
      {value, message.cas}
    else
      value
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

  defp recv_message(socket) do
    with {:ok, message} <- recv_header(socket) do
      recv_body(message, socket)
    end
  end

  defp socket_send(iolist, socket) do
    :ok = :gen_tcp.send(socket, iolist)
  end

end
