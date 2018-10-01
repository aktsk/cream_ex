require IEx

defmodule Cream.ClientTest do
  use ExUnit.Case

  # import ExUnit.CaptureLog # Used to capture logging and assert against it.

  alias Cream.Test.Client

  setup do
    Client.flush_all
    :ok
  end

  test "set and get" do
    assert Client.get!("name") == nil
    assert Client.set("name", "Callie") == {:ok, :stored}
    assert Client.get!("name") == "Callie"
  end

  test "cas" do
    Client.set("name", "Callie")
    {"Callie", cas} = Client.gets!("name")
    assert Client.cas("name", "Coco", cas) == {:ok, :stored}
    assert Client.cas("name", "Genevieve", cas) == {:ok, :exists}
  end

  test "add" do
    assert Client.add("name", "Callie") == {:ok, :stored}
    assert Client.add("name", "Coco") == {:ok, :not_stored}
  end

  test "replace" do
    assert Client.replace("name", "Callie") == {:ok, :not_stored}
    assert Client.set("name", "Callie") == {:ok, :stored}
    assert Client.replace("name", "Coco") == {:ok, :stored}
  end

  test "mset" do
    assert Client.get(["name", "species"]) == {:ok, %{}}
    assert Client.mset(%{"name" => "Callie", "species" => "canine"}) == {:ok, :stored}
    assert Client.get(["name", "species"]) == {:ok, %{"name" => "Callie", "species" => "canine"}}
  end

end
