# Usage:
# $ mix run ./lib/bigquery_gateway/es_2bq_copier.exs --es-endpoint "http://es-hist-test-01.totango:9200" --es-index "880_2019_05_1_idx" --es-days-back 1 --bq-project "s" --bq-dataset "dataset" --bq-table "mytable123"
# Hit Ctrl+C twice to stop it.

defmodule EsFetcher do
  @moduledoc """
  Fetches documents from elasticsearch using the scroll API.
  """
  use GenStage

  def start_link(initial) do
    GenStage.start_link(__MODULE__, initial, name: __MODULE__)
  end

  ## Callbacks

  def init(%{endpoint: endpoint, index: index, days_back: days_back, scroll_size: scroll_size}) do
    IO.puts("### EsFetcher launching ###")
    # Init scroll connection to elasticsearch, and retrieve first batch
    case BigqueryGateway.ElasticsearchUtils.initialize_scroll(endpoint, index,
           size: scroll_size,
           days_back: days_back
         ) do
      {:continue, %{docs: docs, scroll_id: scroll_id}} ->
        {:producer, %{scroll_id: scroll_id, docs: docs, endpoint: endpoint}}

      {:complete, _scroll_id} ->
        # GenStage.async_info(self(), {:producer, :done})
        # {:producer, :done}
        {:stop, "No documents found in index #{index}, #{days_back} days back"}

      {:error, reason} ->
        {:stop, "EsFetcher could not establish connection to elasticsearch"}
    end
  end

  def handle_demand(demand, state) when demand > 0 do
    {docs, scroll_id} =
      if length(state.docs) >= demand do
        {state.docs, state.scroll_id}
      else
        missing_demand = demand - length(state.docs)
        IO.puts("### EsFetcher fetching more docs ###")

        case fetch_missing_demand(state.endpoint, state.scroll_id, missing_demand) do
          {[], _scroll_id} ->
            IO.puts("EsFetcher reached end of docs")
            GenStage.async_info(self(), :stop)
            {state.docs, state.scroll_id}

          {docs, scroll_id} ->
            IO.puts("EsFetcher fetched #{length(docs)} docs")
            {docs, scroll_id}
        end
      end

    # We dispatch only the requested number of events.
    {to_dispatch, remaining} = Enum.split(docs, demand)
    IO.puts("EsFetcher.handle_demand returning #{length(to_dispatch)} events")
    {:noreply, to_dispatch, %{state | docs: remaining, scroll_id: scroll_id}}
  end

  def handle_info(:stop, state) do
    IO.puts("EsFetcher.handle_inf called with :stop")
    {:stop, :normal, state}
  end

  defp fetch_missing_demand(endpoint, scroll_id, missing_demand) do
    case BigqueryGateway.ElasticsearchUtils.get_next_scroll(endpoint, scroll_id) do
      {[], scroll_id} ->
        {[], scroll_id}

      {docs, scroll_id} ->
        fetch_missing_demand(endpoint, scroll_id, missing_demand - length(docs), docs)
    end
  end

  # fetched at least demanded number of documents
  def fetch_missing_demand(_endpoint, scroll_id, missing_demand, docs) when missing_demand <= 0 do
    {docs, scroll_id}
  end

  def fetch_missing_demand(_endpoint, scroll_id, missing_demand, docs)
      when missing_demand >= 0 and length(docs) == 0 do
    {docs, scroll_id}
  end

  def fetch_missing_demand(endpoint, scroll_id, missing_demand, docs) when missing_demand >= 0 do
    case BigqueryGateway.ElasticsearchUtils.get_next_scroll(endpoint, scroll_id) do
      {[], _scroll_id} ->
        {docs, :done}

      {ret_docs, scroll_id} ->
        fetch_missing_demand(
          endpoint,
          scroll_id,
          missing_demand - length(ret_docs),
          docs ++ ret_docs
        )
    end
  end
end

defmodule BqStreamer do
  use GenStage

  @min_demand 1800
  @max_demand 2000

  def start_link(initial) do
    GenStage.start_link(__MODULE__, initial)
  end

  def init(%{project: project, dataset: dataset, min_batch_size: min_batch_size, table: table}) do
    IO.puts("BqStreamer subscribing to EsFetcher")

    {:consumer,
     %{
       project: project,
       dataset: dataset,
       table: table,
       min_batch_size: min_batch_size,
       rows: []
     }, subscribe_to: [{EsFetcher, min_demand: @min_demand, max_demand: @max_demand}]}
  end

  def handle_events(rows, _from, state) do
    rows_to_stream = rows ++ state.rows
    IO.puts("BqStreamer accumulated rows: #{length(rows_to_stream)}")

    case length(rows_to_stream) >= state.min_batch_size do
      true ->
        IO.puts("reached min batch size, streaming to bigquery")
        # BigqueryGateway.stream_into_table(state.project, state.dataset, state.table, rows)
        IO.puts("done")
        {:noreply, [], %{state | rows: []}}

      false ->
        IO.puts("batch size too small, accumulating for next round")
        {:noreply, [], %{state | rows: rows_to_stream}}
    end
  end

  def handle_info(message, state) do
    IO.puts("\nBqStreame.handle_info called with #{inspect(message)}\n")
    {:stop, :normal, state}
  end
end

defmodule Consumer do
  @moduledoc """
  A consumer will be a consumer supervisor that will
  spawn printer tasks for each event.
  """

  use ConsumerSupervisor

  def start_link() do
    ConsumerSupervisor.start_link(__MODULE__, :ok)
  end

  # Callbacks

  def init(:ok) do
    children = [
      worker(Printer, [], restart: :temporary)
    ]

    {:ok, children, strategy: :one_for_one, subscribe_to: [{EsFetcher, max_demand: 50}]}
  end
end

defmodule Printer do
  def start_link(event) do
    Task.start_link(fn ->
      IO.inspect({self(), event})
    end)
  end
end

defmodule App do
  @moduledoc """
  Application entry-point.

  For actual applications, start/0 should be start/2.
  """

  @required_params [:es_endpoint, :es_index, :es_days_back, :bq_project, :bq_dataset, :bq_table]

  def start({:error, reason}), do: IO.puts("\n#{reason}\n")

  def start({:ok, options}) do
    import Supervisor.Spec

    IO.puts("\nApplication launched with\n#{inspect(options)}\n")
    endpoint = Keyword.get(options, :es_endpoint)
    index = Keyword.get(options, :es_index)
    days_back = Keyword.get(options, :es_days_back)
    scroll_size = Keyword.get(options, :es_scroll_size, 2000)
    project = Keyword.get(options, :bq_project)
    dataset = Keyword.get(options, :bq_dataset)
    table = Keyword.get(options, :bq_table)
    min_batch_size = Keyword.get(options, :bq_batch_size, 1000)

    children = [
      worker(EsFetcher, [
        %{endpoint: endpoint, index: index, days_back: days_back, scroll_size: scroll_size}
      ]),
      # We can add as many consumer supervisors as consumers as we want!
      # worker(Consumer, [], id: 1)
      # worker(BqStreamer, [], id: 1)
      worker(BqStreamer, [
        %{project: project, dataset: dataset, table: table, min_batch_size: min_batch_size}
      ])
    ]

    Supervisor.start_link(children, strategy: :one_for_one)
  end

  def parse_options() do
    # https://hexdocs.pm/elixir/OptionParser.html#parse/2
    # {cmd_options, _args, invalid} =
    OptionParser.parse(
      System.argv(),
      switches: [
        # es-endpoint "http://..."
        es_endpoint: :string,
        es_index: :string,
        es_days_back: :integer,
        es_scroll_size: :integer,
        bq_project: :string,
        bq_dataset: :string,
        bq_table: :string,
        bq_batch_size: :integer
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
    "parameters:\n\t--es-endpoint <string> --es-index <string> --es-days-back <integer> --bq-project <string> --bq-dataset <string> --bq-table <string>\n\Optional parameters:\n\t--es-scroll-size <integer> --bq-batch-size <integer>"
  end
end

# Start the app and wait forever
App.parse_options()
|> App.validate_options()
|> App.start()

Process.sleep(:infinity)
