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
    Enum.each [AsciiClient, BinaryClient], fn client ->
      client.flush
      assert client.set({"foo", value}) == {:error, "object too large for cache"}
    end
  end

  test "mset server error" do
    value = String.duplicate("x", 1024*1024)

    Enum.each [AsciiClient, BinaryClient], fn client ->
      client.flush

      keys_and_values = [
        {"foo", value},
        {"bar", "bar"},
        {"baz", value}
      ]

      assert client.set(keys_and_values) == {:error, %{
        "foo" => "object too large for cache",
        "baz" => "object too large for cache"
      }}
    end
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

  test "add" do
    Enum.each [AsciiClient, BinaryClient], fn client ->
      client.flush

      assert client.add({"name", "Callie"}) == {:ok, :stored}
      assert client.get("name") == {:ok, "Callie"}

      assert client.add({"name", "Coco"}) == {:error, :not_stored}
      assert client.get("name") == {:ok, "Callie"}
    end
  end

  test "madd" do
    Enum.each [AsciiClient, BinaryClient], fn client ->
      client.flush

      keys_and_values = %{
        "name" => "Callie",
        "species" => "canine"
      }

      client.set({"name", "Callie"})

      assert client.add(keys_and_values) == {:error, %{
        "name" => :not_stored
      }}
    end
  end

  test "replace" do
    Enum.each [AsciiClient, BinaryClient], fn client ->
      client.flush

      assert client.replace({"name", "Callie"}) == {:error, :not_stored}
      client.set({"name", "Callie"})
      assert client.replace({"name", "Coco"}) == {:ok, :stored}
    end
  end

  test "multi-replace" do
    Enum.each [AsciiClient, BinaryClient], fn client ->
      client.flush

      client.set({"name", "Callie"})

      keys_and_values = %{"name" => "Coco", "species" => "canine"}

      assert client.replace(keys_and_values) == {:error, %{
        "species" => :not_stored
      }}

      client.set({"species", "canine"})

      assert client.replace(keys_and_values) == {:ok, :stored}
    end
  end

end
