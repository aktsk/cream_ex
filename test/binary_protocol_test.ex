defmodule BinaryProtocolTest do

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
