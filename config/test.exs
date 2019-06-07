use Mix.Config

config :logger,
  level: :info

config :cream, Cream.Test.BinaryConnection,
  server: "localhost:11201",
  protocol: :binary

config :cream, Cream.Test.AsciiConnection,
  server: "localhost:11201",
  protocol: :ascii
