defmodule BigqueryGateway.ElasticsearchUtils do
  @default_docs_size 0
  @default_days_back 1

  @doc """
  List indices in elasticsearch cluster
  ## Example

      iex> BigqueryGateway.ElasticsearchUtils.list_indices("http://es-hist-test-01.totango:9200")
  """
  def list_indices(endpoint) do
    HTTPoison.get("#{endpoint}/_cat/indices")
    |> extract_body()
    |> parse_indices()
    |> encode_response()
  end

  # curl -s -XPOST http://es-hist-test-01.totango:9200/160_2019_04_1_idx/_search -d '{ "query": { "range" : { "local_date" : { "gte" : "2019-04-20" } } } }'
  # curl -s -XPOST http://es-hist-test-01.totango:9200/160_2019_04_1_idx/_search -d '{ "query": { "range" : { "date" : { "gte" : "now-2d/d" } } } }'

  @doc """
  Retrieve documents from a specific index
  ## Example

      iex> BigqueryGateway.ElasticsearchUtils.get_docs("http://es-hist-test-01.totango:9200", "880_2019_04_1_idx", [size: 1, days_back: 5])
  """
  def get_docs(endpoint, index, options \\ []) do
    size = Keyword.get(options, :size, @default_docs_size)
    days_back = Keyword.get(options, :days_back, @default_days_back)

    # body = "{ \"size\" : #{size}, \"query\": { \"range\" : { \"date\" : { \"gte\" : \"now-#{days_back}d/d\" } } } }"
    body =
      "{ \"size\" : #{size}, \"query\": { \"range\" : { \"date\" : { \"gte\" : \"now-#{days_back}d/d\" } } }, \"sort\": [ {\"date\": \"asc\"}] }"

    # HTTPoison.post("#{endpoint}/#{index}/_search?size=#{size}",body,[{"Content-Type", "application/json"}])
    HTTPoison.post("#{endpoint}/#{index}/_search", body, [{"Content-Type", "application/json"}])
    |> extract_body()
    |> extract_hits()
    |> extract_sources()
    |> convert_field_to_bq_timestamp("date")
    |> encode_and_delimit()
  end

  def initialize_scroll(endpoint, index, options \\ []) do
    size = Keyword.get(options, :size, @default_docs_size)
    days_back = Keyword.get(options, :days_back, @default_days_back)
    # sort by _doc is most efficient for iterating over all docs when order does noty matter
    body =
      "{ \"size\" : #{size}, \"query\": { \"range\" : { \"date\" : { \"gte\" : \"now-#{days_back}d/d\" } } }, \"sort\": [ \"_doc\" ] }"

    HTTPoison.post("#{endpoint}/#{index}/_search?scroll=1m", body, [
      {"Content-Type", "application/json"}
    ])
    |> extract_scrolled_body()
    |> extract_scrolled_hits()
    |> extract_scrolled_docs()
    |> convert_to_bq_timestamp("date")

    # |> package_state_for_consumer(endpoint)
  end

  defp package_state_for_consumer({:continue, %{docs: converted_docs, scroll_id: scroll_id}}) do
    {converted_docs, scroll_id}
  end

  defp package_state_for_consumer({:complete, scroll_id}) do
    {[], scroll_id}
  end

  defp package_state_for_consumer({:error, reason}) do
    {:error, reason}
  end

  @doc """
  Retrieve documents from a specific index

  ## Parameters

    - endpoint: "http://host:port"
    - index:  "name_of_index"
    - options: [size: 1000, days_back: 5] will iterate over all documents from 5 days ago till now, in bulks of 1000 docs

  ## Example

      iex> BigqueryGateway.ElasticsearchUtils.get_docs_scroll(
        "http://es-hist-test-01.totango:9200", 
        "880_2019_04_1_idx", 
        [size: 1000, days_back: 2])
  """
  def get_docs_scroll(endpoint, index, options \\ []) do
    #
    # https://www.elastic.co/guide/en/elasticsearch/reference/current/search-request-scroll.html
    # add ?scroll=1m in the query string
    # result will contain a _scroll_id, which should be used in subsequent calls:
    # POST /_search/scroll
    #  {
    #      "scroll" : "1m",
    #      "scroll_id" : "DXF1ZXJ5QW5kRmV0Y2gBAAAAAAAAAD4WYm9laVYtZndUQlNsdDcwakFMNjU1QQ=="
    #  }
    #
    #  The subsequent scroll POSTs should not include the index !
    #
    size = Keyword.get(options, :size, @default_docs_size)
    days_back = Keyword.get(options, :days_back, @default_days_back)
    # sort by _doc is most efficient for iterating over all docs when order does noty matter
    body =
      "{ \"size\" : #{size}, \"query\": { \"range\" : { \"date\" : { \"gte\" : \"now-#{days_back}d/d\" } } }, \"sort\": [ \"_doc\" ] }"

    HTTPoison.post("#{endpoint}/#{index}/_search?scroll=1m", body, [
      {"Content-Type", "application/json"}
    ])
    |> extract_scrolled_body()
    |> extract_scrolled_hits()
    |> extract_scrolled_docs()
    |> convert_to_bq_timestamp("date")
    |> iterate_to_completion(endpoint)
  end

  def get_next_scroll(endpoint, scroll_id) do
    body = "{ \"scroll\" : \"1m\" , \"scroll_id\" : \"#{scroll_id}\" }"

    HTTPoison.post("#{endpoint}/_search/scroll", body, [{"Content-Type", "application/json"}])
    |> extract_scrolled_body()
    |> extract_scrolled_hits()
    |> extract_scrolled_docs()
    |> convert_to_bq_timestamp("date")
    |> package_state_for_consumer()
  end

  defp iterate_to_completion({:continue, %{docs: converted_docs, scroll_id: scroll_id}}, endpoint) do
    # defp iterate_to_completion({:continue, scroll_id}, endpoint) do
    IO.puts("\nNext scroll iteration, with scroll_id: #{scroll_id}")
    body = "{ \"scroll\" : \"1m\" , \"scroll_id\" : \"#{scroll_id}\" }"

    HTTPoison.post("#{endpoint}/_search/scroll", body, [{"Content-Type", "application/json"}])
    |> extract_scrolled_body()
    |> extract_scrolled_hits()
    |> extract_scrolled_docs()
    |> iterate_to_completion(endpoint)
  end

  defp iterate_to_completion({:complete, scroll_id}, endpoint) do
    # be civil and clean up after yourself - delete scroll_id
    IO.puts("Scroll complete, deleting scroll_id: #{scroll_id}")
    # body = "{ \"scroll_id\" : \"#{scroll_id}\" }"
    {
      :ok,
      %HTTPoison.Response{
        body: _body,
        headers: _headers,
        request: _request,
        request_url: _request_url,
        status_code: 200
      }
    } =
      HTTPoison.delete("#{endpoint}/_search/scroll/#{scroll_id}", [
        {"Content-Type", "application/json"}
      ])

    IO.puts("deleted.")
  end

  defp iterate_to_completion({:error, reason}, endpoint) do
    IO.puts("Error during elasticsearch iteration\nendpoint: #{endpoint}\nreason:  #{reason}")
  end

  defp extract_scrolled_body({:error, %HTTPoison.Error{} = response}) do
    {:error, "#{inspect(response)}"}
  end

  defp extract_scrolled_body(
         {:ok,
          %HTTPoison.Response{
            body: body,
            headers: _headers,
            request: _request,
            request_url: _request_url,
            status_code: _status_code
          }}
       ) do
    decoded_body = Poison.decode!(body)
    {:ok, decoded_body}
  end

  defp extract_scrolled_hits(
         {:ok,
          %{
            "_scroll_id" => scroll_id,
            "_shards" => %{"failed" => _failed, "successful" => _successful, "total" => _total},
            "hits" => %{"hits" => hits},
            "timed_out" => _timed_out,
            "took" => _took
          }}
       ) do
    #IO.puts("\nReceived #{Enum.count(hits)} hits\n")
    {:ok, %{scroll_id: scroll_id, hits: hits}}
  end

  defp extract_scrolled_hits(
         {:ok,
          %{
            "error" => %{"reason" => reason, "root_cause" => _root_cause, "type" => _type},
            "status" => _status
          }}
       ) do
    {:error, "#{reason}"}
  end

  defp extract_scrolled_hits({:error, reason}) do
    {:error, reason}
  end

  defp extract_scrolled_docs({:error, reason}) do
    IO.puts("scroll error: #{inspect(reason)}")
    {:error, reason}
  end

  defp extract_scrolled_docs({:complete, scroll_id}) do
    # end of scroll, be responsible, and DELETE scroll_id
    # DELETE /_search/scroll -d '{ "scroll_id" : "the-scroll-id-123" }'
    IO.puts("Reached end of scroll for scroll_id: #{scroll_id}")
    {:complete, scroll_id}
  end

  defp extract_scrolled_docs({:ok, %{scroll_id: scroll_id, hits: []}}) do
    # end of scroll, be responsible, and DELETE scroll_id
    # DELETE /_search/scroll -d '{ "scroll_id" : "the-scroll-id-123" }'
    IO.puts("Reached end of scroll for scroll_id: #{scroll_id}")
    {:complete, scroll_id}
  end

  defp extract_scrolled_docs({:ok, %{scroll_id: scroll_id, hits: hits}}) do
    docs =
      hits
      |> Enum.map(fn %{
                       "_id" => _id,
                       "_index" => _index,
                       "_score" => _score,
                       "_source" => source,
                       "_type" => _type
                     } ->
        source
      end)

    {:continue, %{docs: docs, scroll_id: scroll_id}}
  end

  defp convert_to_bq_timestamp({:complete, scroll_id}) do
    {:complete, scroll_id}
  end

  defp convert_to_bq_timestamp({:complete, scroll_id}, _field_name) do
    {:complete, scroll_id}
  end

  defp convert_to_bq_timestamp({:error, reason}, _field_name) do
    {:error, reason}
  end

  defp convert_to_bq_timestamp({:continue, %{docs: docs, scroll_id: scroll_id}}, field_name) do
    converted_docs =
      docs
      |> Enum.map(fn x ->
        date = Map.get(x, field_name)
        Map.put(x, field_name, to_bq_timestamp(date))
      end)

    {:continue, %{docs: converted_docs, scroll_id: scroll_id}}
  end

  defp extract_body({:error, %HTTPoison.Error{} = response}) do
    {:error, "#{inspect(response)}"}
  end

  defp extract_body(
         {:ok,
          %HTTPoison.Response{
            body: body,
            headers: _headers,
            request: _request,
            request_url: _request_url,
            status_code: _status_code
          }}
       ) do
    {:ok, body}
  end

  defp parse_indices({:ok, body}) do
    indices =
      body
      |> String.split(~r{\n})
      |> Enum.map(fn x -> String.split(x) end)
      |> Enum.map(fn x -> Enum.at(x, 2) end)

    {:ok, indices |> Enum.sort()}
  end

  defp parse_indices({:error, reason}) do
    {:error, reason}
  end

  defp extract_hits({:error, reason}) do
    {:error, reason}
  end

  defp extract_hits({:ok, body}) do
    %{
      "_shards" => %{"failed" => _failed, "successful" => _successful, "total" => _total},
      "hits" => %{"hits" => hits},
      "timed_out" => _timed_out,
      "took" => _took
    } = body |> Poison.decode!()

    {:ok, hits}
  end

  defp extract_sources({:error, reason}) do
    {:error, reason}
  end

  defp extract_sources({:ok, hits}) do
    docs =
      hits
      |> Enum.map(fn %{
                       "_id" => _id,
                       "_index" => _index,
                       "_score" => _score,
                       "_source" => source,
                       "_type" => _type
                     } ->
        source
      end)

    {:ok, docs}
  end

  defp convert_field_to_bq_timestamp({:error, reason}, _field_name) do
    {:error, reason}
  end

  defp convert_field_to_bq_timestamp({:ok, docs}, field_name) do
    converted_sources =
      docs
      |> Enum.map(fn x ->
        date = Map.get(x, field_name)
        Map.put(x, field_name, to_bq_timestamp(date))
      end)

    {:ok, converted_sources}
  end

  # elasticsearch timestamps are in milliseconds, while BQ expects it in microseconds or seconds
  defp to_bq_timestamp(maybe_timestamp) when is_integer(maybe_timestamp) do
    case maybe_timestamp |> to_string() |> String.length() do
      # 16 -> maybe_timestamp
      # 13 -> (maybe_timestamp * 1000)
      # 10 -> (maybe_timestamp * 1000000)
      16 -> trunc(maybe_timestamp / 1_000_000)
      13 -> trunc(maybe_timestamp / 1000)
      10 -> maybe_timestamp
    end
  end

  defp encode_and_delimit({:ok, sources}) do
    sources
    |> Poison.encode!()
  end

  defp encode_and_delimit({:error, reason}) do
    IO.puts("Error: #{inspect(reason)}")
  end

  defp encode_response({:ok, indices}) do
    {:ok, indices}
  end

  defp encode_response({:error, reason}) do
    reason
  end
end
