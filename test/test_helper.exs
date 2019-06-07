ExUnit.start()
{:ok, _} = Cream.Test.BinaryConnection.start_link
{:ok, _} = Cream.Test.AsciiConnection.start_link
