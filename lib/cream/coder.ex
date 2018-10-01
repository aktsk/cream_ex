defmodule Cream.Coder do
  @type flags :: integer
  @type value :: binary

  @callback encode(value) :: {flags, value}
  @callback decode(flags, value) :: value
end
