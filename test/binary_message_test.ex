defmodule BinaryMessageTest do

  use ExUnit.Case

  alias Cream.Protocol.Binary.Message

  test "message -> iolist -> binary" do
    extras = [flags: 1, ttl: 123]

    message = Message.new(:set, key: "foo", value: "bar", extras: extras)

    iolist = Message.to_iolist(message)
    binary = :erlang.iolist_to_binary(iolist)

    assert byte_size(binary) == Enum.sum([
      24, 4, 4, byte_size(message.key), byte_size(message.value)
    ])
  end

end
