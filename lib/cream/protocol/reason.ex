defmodule Cream.Protocol.Reason do
  @moduledoc false
  # The ascii protocol and binary protocol return different "reasons" for errors and statuses.
  # This module normalizes everything to what the ascii protocol says.

  def tr("STORED"), do: :stored

  def tr("NOT_FOUND"), do: :not_found

  def tr("Data exists for key."), do: :not_stored
  def tr("NOT_STORED"), do: :not_stored

  def tr("Too large."), do: "object too large for cache"
end
