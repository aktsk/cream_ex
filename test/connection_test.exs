defmodule ApiTests do

  defmacro __using__(client) do
    quote location: :keep do
      use ExUnit.Case

      @moduletag client: unquote(client)

      setup %{client: client} do
        assert client.flush == {:ok, :flushed}
        :ok
      end

      test "set and get", %{client: client} do
        assert client.get("foo") == {:ok, nil}
        assert client.set({"foo", "bar"}) == {:ok, :stored}
        assert client.get("foo") == {:ok, "bar"}
      end

      test "multi set and multi get", %{client: client} do
        assert client.get(["foo", "bar"]) == {:ok, %{}}
        assert client.set(%{"foo" => "oof", "bar" => "rab"}) == {:ok, %{
          stored: ["foo", "bar"]
        }}
        assert client.get(["foo", "bar"]) == {:ok, %{
          "foo" => "oof",
          "bar" => "rab"
        }}
      end

    end
  end

end

defmodule BinaryConnectionTest do
  use ApiTests, Cream.Test.BinaryConnection
end

defmodule AsciiConnectionTest do
  use ApiTests, Cream.Test.AsciiConnection
end
