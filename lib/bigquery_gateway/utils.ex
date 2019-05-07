defmodule BigqueryGateway.Utils do
  @doc """
  Create a http friendly nonce

  ## Parameters

  - length: optional, determines the number of bytes on which to do the crypto random, defaults to 16 bytes 
  """
  def nonce(length \\ 16) do
    :crypto.strong_rand_bytes(length) |> Base.url_encode64() |> binary_part(0, length)
  end

  @doc """
  Get human friendly current UTC time in bigquery compatible form
  """
  def current_timestamp() do
    # "2019-04-21 08:42:15 +03:00"
    DateTime.utc_now() |> DateTime.to_string()
  end

  ##################################################
  # Throwable functions
  #
  def generate_random_row() do
    [
      %{
        nonce: "#{BigqueryGateway.Utils.nonce()}",
        created_at: "#{BigqueryGateway.Utils.current_timestamp()}",
        updated_at: "#{BigqueryGateway.Utils.current_timestamp()}",
        service_id: "service_123",
        entity_type: "account",
        attributes: generate_random_attributes()
      }
    ]
    |> Poison.encode!()
  end

  def get_random_field_and_type() do
    Enum.random([
      {"aaa", "string"},
      {"bbb", "string"},
      {"ccc", "integer"},
      {"ggg", "string"},
      {"nnn", "string"},
      {"zzz", "integer"}
    ])
  end

  def get_random_field_value("string") do
    Enum.random([
      "little",
      "red",
      "fox",
      "ran",
      "through",
      "woods"
    ])
  end

  def get_random_field_value("integer") do
    get_random_integer()
  end

  def get_random_attributes_number() do
    Integer.mod(Enum.random(1..20), 4) + 1
  end

  def get_random_integer(integer \\ 100) do
    Enum.random(0..integer)
  end

  def generate_random_attributes() do
    Enum.map(1..BigqueryGateway.Utils.get_random_attributes_number(), fn _x ->
      {field_name, field_type} = get_random_field_and_type()
      field_value = get_random_field_value(field_type)
      %{name: field_name, type: field_type, value: field_value}
    end)
  end
end
