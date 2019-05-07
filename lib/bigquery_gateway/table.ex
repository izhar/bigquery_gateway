defmodule BigqueryGateway.Table do
  alias BigqueryGateway.Connection

  def create(project_id, dataset, table_name, schema, options \\ []) do
    google_schema = schema |> convert_to_google_struct()

    GoogleApi.BigQuery.V2.Api.Tables.bigquery_tables_insert(
      Connection.get(),
      project_id,
      dataset,
      body: %GoogleApi.BigQuery.V2.Model.Table{
        tableReference: %GoogleApi.BigQuery.V2.Model.TableReference{
          datasetId: dataset,
          projectId: project_id,
          tableId: table_name
        },
        schema: %GoogleApi.BigQuery.V2.Model.TableSchema{
          fields: google_schema
        },
        timePartitioning: get_partition_on(Keyword.get(options, :partition_on)),
        clustering: get_cluster_on(Keyword.get(options, :cluster_on))
      }
    )
    |> process_create_table_response()
  end

  defp convert_to_google_struct(schema) do
    schema
    |> cast_to_bq_schema()
  end

  def get_partition_on(nil) do
    nil
  end

  def get_partition_on(partition_on) do
    %GoogleApi.BigQuery.V2.Model.TimePartitioning{
      type: "DAY",
      field: partition_on
    }
  end

  def get_cluster_on(nil) do
    nil
  end

  def get_cluster_on(cluster_on) do
    %GoogleApi.BigQuery.V2.Model.Clustering{
      fields: cluster_on
    }
  end

  defp process_create_table_response({:ok, _response}) do
    IO.puts("Create table succeeded")
  end

  defp process_create_table_response({:error, response}) do
    IO.puts("Create table failed")
    IO.puts("Reason: #{inspect(response)}")
  end

  def stream_into(project_id, dataset, table, payload) do
    # We expect json to be valid (done in outer layers)
    #
    {:ok, decoded_payload} = Poison.decode(payload)
    # The TableDataInsertAllRequest.rows element is expected to be in the format of
    # rows: [ %{json: row1},{json: row2},.. ]
    rows =
      case is_list(decoded_payload) do
        # Payload received is single json row
        false -> [%{json: decoded_payload}]
        # Payload received is an array of json rows
        true -> Enum.map(decoded_payload, fn x -> %{json: x} end)
      end

    stream_request = %GoogleApi.BigQuery.V2.Model.TableDataInsertAllRequest{
      skipInvalidRows: true,
      ignoreUnknownValues: false,
      rows: rows
    }

    GoogleApi.BigQuery.V2.Api.Tabledata.bigquery_tabledata_insert_all(
      Connection.get(),
      project_id,
      dataset,
      table,
      body: stream_request
    )
    |> process_stream_insert()
  end

  defp process_stream_insert({:error, reason}) do
    IO.puts("iinsert failed with reason: #{inspect(reason)}")
    {:error, reason}
  end

  defp process_stream_insert(
         {:ok,
          %GoogleApi.BigQuery.V2.Model.TableDataInsertAllResponse{insertErrors: nil, kind: _kind}}
       ) do
    IO.puts("completed with no errors")
    :ok
  end

  defp process_stream_insert(
         {:ok,
          %GoogleApi.BigQuery.V2.Model.TableDataInsertAllResponse{
            insertErrors: insert_errors,
            kind: _kind
          }}
       ) do
    IO.puts("insert_errors: #{inspect(insert_errors)}")
    IO.puts("completed with errors")
    {:ok, insert_errors}
  end

  defp cast_to_bq_schema(schema) do
    schema |> Enum.map(&cast_to_schema(&1))
  end

  defp cast_to_schema(%{name: name, type: type, mode: mode, fields: fields} = _schema) do
    %GoogleApi.BigQuery.V2.Model.TableFieldSchema{
      name: name,
      type: type,
      mode: mode,
      fields: fields
    }
  end

  defp cast_to_schema(%{name: name, type: type, mode: mode} = _schema) do
    %GoogleApi.BigQuery.V2.Model.TableFieldSchema{name: name, type: type, mode: mode}
  end
end
