defmodule ClientTest do

  use ExUnit.Case

  alias Cream.Test.{AsciiClient, BinaryClient}

  test "get not found" do
    Enum.each [AsciiClient, BinaryClient], fn client ->
      client.flush

      assert client.get("name") == {:ok, nil}
    end
  end

  test "get! not found" do
    Enum.each [AsciiClient, BinaryClient], fn client ->
      client.flush

      assert client.get!("name") == nil
    end
  end

  test "set client error" do
    AsciiClient.flush
    assert AsciiClient.set({"foo", "bar"}, ttl: "tomorrow") == {:error, "bad command line format"}

    # I don't know how to cause a client error with the binary client.
    # TODO cause client error with binary client.
  end

  test "set server error" do
    value = String.duplicate("x", 1024*1024)

    AsciiClient.flush
    assert AsciiClient.set({"foo", value}) == {:error, "object too large for cache"}

    BinaryClient.flush
    assert BinaryClient.set({"foo", value}) == {:error, "Too large."}
  end

  test "mset server error" do
    value = String.duplicate("x", 1024*1024)

    keys_and_values = [
      {"foo", value},
      {"bar", "bar"},
      {"baz", value}
    ]

    AsciiClient.flush
    assert AsciiClient.set(keys_and_values) == {:error, %{
      "foo" => "object too large for cache",
      "baz" => "object too large for cache"
    }}

    BinaryClient.flush
    assert BinaryClient.set(keys_and_values) == {:error, %{
      "foo" => "Too large.",
      "baz" => "Too large."
    }}
  end

  test "set and get" do
    Enum.each [AsciiClient, BinaryClient], fn client ->
      client.flush
      assert client.set({"name", "Callie"}) == {:ok, :stored}
      assert client.get("name") == {:ok, "Callie"}
    end
  end

  test "set! and get!" do
    Enum.each [AsciiClient, BinaryClient], fn client ->
      client.flush
      assert client.set!({"name", "Callie"}) == :stored
      assert client.get!("name") == "Callie"
    end
  end

  test "mset and mget" do
    Enum.each [AsciiClient, BinaryClient], fn client ->
      client.flush

      assert client.get(["name", "species", "foo"]) == {:ok, %{}}

      keys_and_values = %{
        "name" => "Callie",
        "species" => "canine"
      }

      assert client.set(keys_and_values) == {:ok, :stored}

      assert client.get(["name", "species", "foo"]) == {:ok, %{
        "name" => "Callie",
        "species" => "canine",
      }}
    end
  end

  test "mset! and mget!" do
    Enum.each [AsciiClient, BinaryClient], fn client ->
      client.flush

      assert client.get!(["name", "species", "foo"]) == %{}

      keys_and_values = %{
        "name" => "Callie",
        "species" => "canine"
      }

      assert client.set!(keys_and_values) == :stored

      assert client.get!(["name", "species", "foo"]) == %{
        "name" => "Callie",
        "species" => "canine"
      }
    end
  end

end
