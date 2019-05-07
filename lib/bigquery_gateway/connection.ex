defmodule BigqueryGateway.Connection do
  @doc """
  Create a tesla connection to make an API request if doesn't exist, and if exist, 
  it brings the last buffered connection.
  It ALREADY has a token server in Goth (google authentication) library
  """
  def get() do
    {:ok, token} = Goth.Token.for_scope("https://www.googleapis.com/auth/cloud-platform")
    # GoogleApi.Storage.V1.Connection.new(token.token)
    GoogleApi.BigQuery.V2.Connection.new(token.token)
  end
end
