defmodule Cream.Protocol.Binary.Opcode do
  use Agent

  def start_link(_) do
    Agent.start_link(__MODULE__, :init, [], name: __MODULE__)
  end

  def init do
    path = Enum.find :code.get_path, fn path ->
      List.to_string(path) |> String.ends_with?("cream/ebin")
    end

    {:ok, beams} = :erl_prim_loader.list_dir(path)

    Enum.reduce beams, %{}, fn beam, acc ->
      beam = List.to_string(beam)
      if beam =~ ~r/Elixir\.Cream\.Protocol\.Binary\.Packet\.[^.]+\.beam/ do
        module = String.replace_suffix(beam, ".beam", "") |> String.to_atom()
        {:module, module} = Code.ensure_loaded(module)
        opcode = module.specification.opcode
        Map.put(acc, opcode, module)
      else
        acc
      end
    end
  end

  def get_module(opcode) do
    Agent.get(__MODULE__, fn state -> state[opcode] end)
  end

end
