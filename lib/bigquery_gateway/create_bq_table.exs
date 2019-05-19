defmodule BqTable do
  defmodule App do
    @required_params [:project, :dataset, :table, :schema]
    def start({:error, reason}), do: IO.puts("\n#{reason}\n")

    def start({:ok, options}) do
      schema_path = Keyword.get(options, :schema)
      project = Keyword.get(options, :project)
      dataset = Keyword.get(options, :dataset)
      table = Keyword.get(options, :table)
      schema = BigqueryGateway.Utils.schema_from_file(schema_path)
      partition_on = Keyword.get(options, :partition_on, nil)
      cluster_on = Keyword.get(options, :cluster_on, nil)
      IO.puts("creating table #{table} in #{project}.#{dataset}")

      BigqueryGateway.create_table(project, dataset, table, schema,
        partition_on: partition_on,
        cluster_on: cluster_on
      )

      IO.puts("done.")
    end

    def parse_options() do
      # https://hexdocs.pm/elixir/OptionParser.html#parse/2
      # {cmd_options, _args, invalid} =
      OptionParser.parse(
        System.argv(),
        switches: [
          project: :string,
          dataset: :string,
          table: :string,
          schema: :string,
          partition_on: :string,
          cluster_on: :string
        ]
      )
    end

    def validate_options({[], [], []}) do
      {:error, "Required #{usage()}"}
    end

    def validate_options({cmd_options, _, []}) do
      case missing_options(cmd_options) do
        # no missing, continue
        [] ->
          {:ok, cmd_options}

        # print missing and exit
        missing ->
          {:error, "Missing parameters:\n\t#{List.to_string(missing)}\nExpected #{usage()}"}
      end
    end

    def validate_options({_cmd_options, _, invalid}) do
      {:error, "Invalid options supplied: #{inspect(invalid)}\n#{usage()}"}
    end

    def missing_options(cmd_options) do
      (@required_params -- Keyword.keys(cmd_options))
      |> Enum.map(fn x -> to_string(x) end)
      |> Enum.map(fn x -> String.replace(x, "_", "-") end)
      |> Enum.map(fn x -> "--" <> x <> " " end)
    end

    defp usage() do
      "parameters:\n\t--project <string>\n\t--dataset <string>\n\t--table <string>\n\t--schema <string - path to schema json file>\n\Optional parameters:\n\t--partition-on <string - field name to partition on>\n\t--cluster-on <string - field name to cluster on>"
    end
  end

  App.parse_options()
  |> App.validate_options()
  |> App.start()
end
