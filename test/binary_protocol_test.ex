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

      assert client.set(keys_and_values) == %{
        "name" => {:ok, :stored},
        "species" => {:ok, :stored}
      }

      assert client.get(["name", "species", "foo"]) == %{
        "name" => {:ok, "Callie"},
        "species" => {:ok, "canine"},
        "foo" => {:ok, nil}
      }
    end
  end

  # test "mset! and mget!" do
  #   keys_and_values = %{
  #     "name" => "Callie",
  #     "species" => "canine"
  #   }
  #
  #   assert Client.set!(keys_and_values) == %{
  #     "name" => :stored,
  #     "species" => :stored
  #   }
  #
  #   assert Client.get!(["name", "species", "foo"]) == %{
  #     "name" => "Callie",
  #     "species" => "canine"
  #   }
  # end

end
