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

  test "multi-set server error" do
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

  test "multi-set and multi-get" do
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

  test "multi-set! and multi-get!" do
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

  test "multi-add" do
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

  test "delete" do
    Enum.each [AsciiClient, BinaryClient], fn client ->
      client.flush

      keys_and_values = %{"name" => "Callie", "species" => "canine"}

      client.set(keys_and_values)
      assert client.get(["name", "species"]) == {:ok, keys_and_values}

      assert client.delete("species") == {:ok, :deleted}
      assert client.get(["name", "species"]) == {:ok, %{"name" => "Callie"}}
    end
  end

  test "multi delete" do
    Enum.each [AsciiClient, BinaryClient], fn client ->
      client.flush

      keys_and_values = %{"name" => "Callie", "species" => "canine"}
      keys = Map.keys(keys_and_values)

      assert client.set(keys_and_values) == {:ok, :stored}
      assert client.delete(keys) == {:ok, :deleted}
      assert client.get(keys) == {:ok, %{}}

      assert client.set(keys_and_values) == {:ok, :stored}
      assert client.delete(keys ++ ["foo"]) == {:error, %{
        "foo" => :not_found
      }}
      assert client.get(keys) == {:ok, %{}}
    end
  end

  test "cas" do
    Enum.each [AsciiClient, BinaryClient], fn client ->
      client.flush

      assert client.set({"name", "Callie"}) == {:ok, :stored}
      assert {:ok, {"Callie", cas}} = client.get("name", cas: true)
      assert client.set({"name", "Coco"}) == {:ok, :stored}
      assert client.set({"name", {"Genevieve", cas}}) == {:error, :exists}
      {:ok, {"Coco", cas}} = client.get("name", cas: true)
      assert client.set({"name", {"Genevieve", cas}}) == {:ok, :stored}
    end
  end

  test "multi cas" do
    Enum.each [AsciiClient, BinaryClient], fn client ->
      client.flush

      assert client.set(%{
        "name_a" => "Callie1",
        "name_b" => "Coco1",
        "name_c" => "Genevieve1"
      }) == {:ok, :stored}

      {:ok, %{
        "name_a" => {"Callie1", cas_a},
        "name_b" => {"Coco1", cas_b},
        "name_c" => {"Genevieve1", _cas_c}
      }} = client.get(["name_a", "name_b", "name_c"], cas: true)

      assert client.set(%{
        "name_a" => {"Callie2", cas_a},
        "name_b" => {"Coco2", cas_b+1},
        "name_c" => "Genevieve2"
      }) == {:error, %{"name_b" => :exists}}

      assert client.get(["name_a", "name_b", "name_c"]) == {:ok, %{
        "name_a" => "Callie2",
        "name_b" => "Coco1",
        "name_c" => "Genevieve2"
      }}
    end
  end

  test "coder" do
    Enum.each [AsciiClient, BinaryClient], fn client ->
      client.flush

      client.set({"name", "Callie"}, coder: Cream.Coder.Marshal)
      value = client.get!("name")

      assert value != "Callie"
      assert ExMarshal.decode(value) == "Callie"
      assert client.get!("name", coder: Cream.Coder.Marshal) == "Callie"
    end
  end

end
