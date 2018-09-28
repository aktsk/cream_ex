ExUnit.start()
{:ok, _} = Test.Cluster.start_link
{:ok, _} = Cream.Test.Client.start_link
