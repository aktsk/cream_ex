use Mix.Config

config :cream, servers: ["localhost:11211", "127.0.0.1:11211"]

config :cream, :clusters, [
  main: [
    servers: ["127.0.0.1:11211"]
  ]
]

config :cream, Cream.Test.Client, []

config :cream, Cream.Test.AsciiClient, protocol: :ascii
config :cream, Cream.Test.BinaryClient, protocol: :binary
