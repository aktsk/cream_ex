use Mix.Config

config :cream, Test.Cluster,
  servers: ["localhost:11201", "localhost:11202", "localhost:11203"],
  memcachex: [coder: Memcache.Coder.JSON]

config :logger,
  level: :info

config :cream, Cream.Test.BinaryClient,
  server: "localhost:11201",
  protocol: :binary

config :cream, Cream.Test.AsciiClient,
  server: "localhost:11201",
  protocol: :ascii
