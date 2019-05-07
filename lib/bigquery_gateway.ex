defmodule BigqueryGateway do
  @moduledoc """
  Documentation for BigqueryGateway.
  """

  @doc """
  Create a bigquery table with supplied schema

  ## Parameters  

    - project_id: the google project unsder which the dataset resides
    - dataset: the dataset name
    - table_name: name we wish to create for the table
    - schema: a list of schema params describing the table
    - options: an optional list determining which fields to use for time partitioning and/or clustering

  ## Examples

      ## create a table, no time partition, no clusteriing

      iex> BigqueryGateway.create_table(
        "promenade-222313", 
        "integration_hub",
        "entities1", 
        [
          %{name: "nonce", type: "string", mode: "required"},
          %{name: "created_at",type: "timestamp", mode: "required"},
          %{name: "updated_at",type: "timestamp", mode: "nullable"},
          %{name: "service_id", type: "string", mode: "required"},
          %{name: "entity_type", type: "string", mode: "required"},
          %{name: "account_user_id", type: "string", mode: "nullable"},
          %{name: "account_id", type: "string", mode: "nullable"},
          %{name: "attributes", type: "record", mode: "repeated", fields: [
              %{name: "name", type: "string", mode: "nullable"},
              %{name: "type", type: "string", mode: "nullable"},
              %{name: "value", type: "string", mode: "nullable"}
            ]
          }
        ]
      )

      ## create a table with time partition, no clustering

      iex> BigqueryGateway.create_table(
        "promenade-222313", 
        "integration_hub",
        "entities2", 
        [
          %{name: "nonce", type: "string", mode: "required"},
          %{name: "created_at",type: "timestamp", mode: "required"},
          %{name: "updated_at",type: "timestamp", mode: "nullable"},
          %{name: "service_id", type: "string", mode: "required"},
          %{name: "entity_type", type: "string", mode: "required"},
          %{name: "account_user_id", type: "string", mode: "nullable"},
          %{name: "account_id", type: "string", mode: "nullable"},
          %{name: "attributes", type: "record", mode: "repeated", fields: [
              %{name: "name", type: "string", mode: "nullable"},
              %{name: "type", type: "string", mode: "nullable"},
              %{name: "value", type: "string", mode: "nullable"}
            ]
          }
        ],
        [partition_on: "created_at"], "k"
      )

      ## create a table with time partition and clusteriing

      iex> BigqueryGateway.create_table(
        "promenade-222313", 
        "integration_hub",
        "entities3", 
        [
          %{name: "nonce", type: "string", mode: "required"},
          %{name: "created_at",type: "timestamp", mode: "required"},
          %{name: "updated_at",type: "timestamp", mode: "nullable"},
          %{name: "service_id", type: "string", mode: "required"},
          %{name: "entity_type", type: "string", mode: "required"},
          %{name: "account_user_id", type: "string", mode: "nullable"},
          %{name: "account_id", type: "string", mode: "nullable"},
          %{name: "attributes", type: "record", mode: "repeated", fields: [
              %{name: "name", type: "string", mode: "nullable"},
              %{name: "type", type: "string", mode: "nullable"},
              %{name: "value", type: "string", mode: "nullable"}
            ]
          }
        ],
        [partition_on: "created_at", cluster_on: "service_id"]
      )

  """
  defdelegate create_table(project_id, dataset, table_name, schema, options \\ []),
    to: BigqueryGateway.Table,
    as: :create

  @doc """
  Insert row(s) to Google bigquery via the GoogleApi.BigQuery.V2.Api.Tabledata.bigquery_tabledata_insert_all API  

  ## Examples  

        iex> BigqueryGateway.stream_into_table(
          "promenade-222313",
          "integration_hub",
          "users3",
          "[
            {\\"action\\":\\"buy\\",\\"ts\\":\\"2019-04-18T12:30:00.45Z\\",\\"department\\":\\"purchasing\\"}
          ]"
        )

        ## Specifying date in HH:MM:SS format, defaults to UTC timezone

        iex> BigqueryGateway.stream_into_table(
          "promenade-222313",
          "integration_hub",
          "users3",
          "[
            {\\"action\\":\\"buy\\",\\"ts\\":\\"2019-04-21 08:42:15\\",\\"department\\":\\"purchasing\\"}
          ]"
        )

        ## Specifying date in HH:MM:SS format, with timezone offset

        iex> BigqueryGateway.stream_into_table(
          "promenade-222313",
          "integration_hub",
          "users3",
          "[
            {\\"action\\":\\"buy\\",\\"ts\\":\\"2019-04-21 08:42:15 +03:00\\",\\"department\\":\\"purchasing\\"}
          ]"
        )

  """
  defdelegate stream_into_table(project_id, dataset, table_name, rows),
    to: BigqueryGateway.Table,
    as: :stream_into
end
