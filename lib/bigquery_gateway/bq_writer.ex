defmodule BqWriter do
  use GenServer
  require Logger

  defmodule State do
    defstruct [
      :status,
      :reason,
      :rmq_qname,
      :rmq_xname,
      # prefixed to all dataset names in the (multiple) 'envs' environmnet
      :env_prefix,
      :project_id,
      :rmq_channel,
      :rmq_payload,
      :bigquery_sql,
      :bigquery_conn,
      # used by the writer_manager to identify a specific worker
      :genserver_name,
      :bigquery_token,
      :bigquery_dataset,
      :bigquery_table_name,
      :bigquery_table_fields
    ]
  end

  @bigquery_table_fields [
    "timestamp",
    "location_id",
    "fact_id",
    "correlation_id",
    "entity_id",
    "entity_type",
    "source",
    "action",
    "webhook_tags",
    "payload_version",
    "payload"
  ]

  # Instead of using a separate supervisor module
  # Allows us to call {} directly in the application
  #
  # defmodule Writers do
  #
  #   use Application
  #   import Supervisor.Spec
  #
  #   def start(_type,_args) do
  #     children = [ ] 
  #     ++ WriterFactory.get_default_consumers_childspec(:bigquery)
  #     Supervisor.start_link(children, strategy: :one_for_one, name: WriterSupervisor)
  #   end
  #
  # end 
  #

  # Called by the application during its startup
  # def child_spec(%{name: name_in, type: :bigquery}) do
  #  qname = name_in |> to_atom()
  #  name = "#{name_in}_bq" |> to_atom()
  #  %{id: name, start: {__MODULE__, :start_link, [%{name: name, qname: qname, channel: Utils.Rmq.get_amqp_channel(), genserver_name: name}]}, restart: :permanent, type: :worker}
  # end

  # def start_link [] do
  #  IO.puts "BqWriter.start_link called with empty args\n"
  #  GenServer.start_link(__MODULE__, [])
  # end

  # def init([])  do
  #  IO.puts "\BqWriter init/0 called\n"
  #  {:ok,%{}}
  # end

  def start_link(%{domain: domain, prefix: prefix}) do
    Logger.info("\nBqWorker.start_link called with domian: #{domain}, prefix: #{prefix}\n")
    genserver_name = "bq_#{prefix}#{domain}" |> String.trim() |> to_atom()

    GenServer.start_link(
      __MODULE__,
      %{domain: domain, prefix: prefix, genserver_name: genserver_name},
      name: genserver_name
    )

    # GenServer.start_link(__MODULE__, %{domain: domain, prefix: prefix}, name: "bq_#{prefix}#{domain}" |> String.trim() |> to_atom())
  end

  # def start_link(%{name: name, qname: qname, channel: channel, genserver_name: name}) do
  #  IO.puts "BqWorker.start_link called with #{inspect name}"
  #  GenServer.start_link(__MODULE__, %{qname: qname, channel: channel, genserver_name: name}, name: name)
  # end

  def init(args) do
    Process.flag(:trap_exit, true)

    state =
      args
      |> initialize_pipeline()
      |> initialize_rmq_connection()
      |> initialize_bigquery_connection()
      |> maybe_create_dataset()
      |> maybe_create_table()

    {:ok, state}
  end

  # returns a list of workers - one per exchange
  # def get_default_consumers_childspec do
  #  Utils.Rmq.list_xnames_in_config()
  #  |> Enum.map(fn x -> {BqWriter,%{name: x,type: :bigquery}} end)
  # end

  # called by RMQ
  #
  # Confirmation sent by the broker after registering this process as a consumer
  def handle_info({:basic_consume_ok, %{consumer_tag: consumer_tag}}, channel) do
    Logger.info("\n:basic_consume_ok\n")
    Logger.info("BqWriter worker. basic_consume_ok, consumer_tag: #{inspect(consumer_tag)}")
    {:noreply, channel}
  end

  # called when a new event enters the Q
  def handle_info({:basic_deliver, rmq_payload, meta}, state) do
    Logger.info("\nhandle_info :basic_deliver state - #{inspect(state)}\n")
    spawn(fn -> consume(rmq_payload, meta, state) end)
    {:noreply, state}
  end

  # called when a Q is deleted
  # def handle_info({:basic_cancel, rmq_payload, meta}, state) do
  def handle_info(
        {:basic_cancel, %{consumer_tag: consumer_tag, no_wait: true}},
        %{channel: channel, qname: qname}
      ) do
    Logger.info("received cancel from queue #{qname}")
    terminate_writer("#{qname}_bq" |> to_atom())
    {:noreply, "queue deleted"}
  end

  # def list_writers do
  #  Supervisor.which_children(WriterSupervisor)
  # end

  # def add_writer name do
  #  case Supervisor.start_child(WriterSupervisor,{BqWriter, %{name: name, type: :bigquery}}) do
  #    {:error, :already_present} -> 
  #      terminate_writer name
  #    {:error, {:already_started, _pid}} -> 
  #      :ok
  #    {:error,_rest} -> 
  #      Utils.Rmq.delete_queue %{channel: Utils.Rmq.get_amqp_channel(), qname: name}
  #      :ok
  #    _ -> :ok
  #  end
  # end

  def terminate_writer(name) do
    Logger.info("\nterminate_writer called\n")
    # Supervisor.terminate_child(WriterSupervisor,name)
    # Supervisor.delete_child(WriterSupervisor,name)
  end

  # def is_unicode? a_string do
  #  case byte_size(a_string) == String.length(a_string) do
  #    true  ->  false
  #    false ->  true
  #  end
  # end

  # Called when receiving a connection exception.
  # Triggers a connection renewal process
  def handle_cast({:disconnected}, state) do
    Logger.info("\nrenewing bigquery connection\n")
    {:ok, token} = Goth.TokenStore.find("https://www.googleapis.com/auth/cloud-platform")
    conn = GoogleApi.BigQuery.V2.Connection.new(token.token)
    Logger.info("\nconnection re-establishment done.\n")
    {:noreply, %{state | bigquery_conn: conn, bigquery_token: token}}
  end

  def handle_cast(val, state) do
    Logger.info("\nhandle cast called with val: #{inspect(val)}, state: #{inspect(state)}\n")
    {:noreply, state}
  end

  def terminate(reason, state) do
    Logger.info("\n#{inspect(state.genserver_name)} going Down: #{inspect(state)}")
    Logger.info("cleanup - delete my queue\n")
    # Utils.Rmq.delete_queue(%{channel: state.rmq_channel, qname: state.rmq_qname})
    :normal
  end

  ## private ##
  #############

  # defp consume(rmq_payload, %{delivery_tag: tag, priority: _priority, redelivered: _redelivered}, _state = %{name: _name, channel: channel}) do
  defp consume(
         rmq_payload,
         %{delivery_tag: tag, priority: _priority, redelivered: _redelivered},
         state
       ) do
    Logger.info("\nconsumed: #{inspect(rmq_payload)}\n")

    case stream_into_table(%{state | rmq_payload: rmq_payload}) do
      :ok -> Utils.Rmq.basic_ack(%{channel: state.rmq_channel, tag: tag})
      :error -> Utils.Rmq.basic_nack(%{channel: state.rmq_channel, tag: tag})
    end
  end

  # defp write_to_bigquery(state) do
  #  state
  #  |> stream_into_table()
  #  #|> compose_sql()
  #  #|> sync_query()
  # end

  # defp store_in_bigquery state do
  #  state
  #  |> initialize_connection_to_bigquery()
  #  |> maybe_create_dataset()
  #  #|> maybe_create_table()
  #  #|> compose_sql_old()
  #  #|> sync_query()
  # end

  defp maybe_create_dataset(state) do
    Logger.info("\nmaybe create dataset state: #{inspect(state)}\n")
    # {:ok, token}  = Goth.Token.for_scope("https://www.googleapis.com/auth/cloud-platform")
    # conn = GoogleApi.BigQuery.V2.Connection.new(token.token)
    {status, response} =
      GoogleApi.BigQuery.V2.Api.Datasets.bigquery_datasets_insert(
        # conn, 
        # "como-env05", 
        state.bigquery_conn,
        state.project_id,
        body: %GoogleApi.BigQuery.V2.Model.Dataset{
          datasetReference: %GoogleApi.BigQuery.V2.Model.DatasetReference{
            datasetId: "#{state.bigquery_dataset}"
          }
        }
      )

    case status do
      :ok ->
        Logger.info("\ncreated Dataset #{state.bigquery_dataset}\n")
        state

      :error ->
        Logger.info(
          "\ndid not create Dataset #{state.bigquery_dataset}, response: #{inspect(response)}\n"
        )

        %Tesla.Env{
          __client__: _client,
          __module__: _module,
          # example body
          # body: "{\"error\": {\"errors\": [{\"domain\":\"global\",\"reason\":\"duplicate\",\"message\":\"Already Exists: Dataset como-env05:a\"}],\"code\":409,\"message\":\"Already Exists: Dataset como-env05:a\"}}", 
          body: body,
          headers: _headers,
          method: _method,
          opts: _opts,
          query: _query,
          status: status,
          url: _url
        } = response

        body_map = body |> Poison.decode!()

        Logger.info(
          "\ndid not create Dataset #{state.bigquery_dataset}, status: #{status}, reason: #{
            body_map["error"]["message"]
          }\n"
        )

        case status do
          409 -> state
          _other -> %{state | status: :error, reason: body_map["error"]["message"]}
        end
    end
  end

  def maybe_create_table(state) do
    Logger.info("\nmaybe_create_table state: #{inspect(state)}\n")
    conn = state.bigquery_conn
    project_id = state.project_id
    dataset = state.bigquery_dataset
    table = state.bigquery_table_name

    {status, response} =
      GoogleApi.BigQuery.V2.Api.Tables.bigquery_tables_insert(
        conn,
        project_id,
        dataset,
        body: %GoogleApi.BigQuery.V2.Model.Table{
          id: "#{project_id}:#{dataset}.#{table}",
          schema: %GoogleApi.BigQuery.V2.Model.TableSchema{
            fields: [
              # @bigquery_table_fields ["timestamp","location_id","fact_id","correlation_id","entity_id","entity_type","source","action","webhook_tags","payload_version","payload"]
              %GoogleApi.BigQuery.V2.Model.TableFieldSchema{
                name: "timestamp",
                type: "timestamp",
                mode: "required"
              },
              %GoogleApi.BigQuery.V2.Model.TableFieldSchema{
                name: "location_id",
                type: "string",
                mode: "nullable"
              },
              %GoogleApi.BigQuery.V2.Model.TableFieldSchema{
                name: "business_id",
                type: "string",
                mode: "nullable"
              },
              %GoogleApi.BigQuery.V2.Model.TableFieldSchema{
                name: "fact_id",
                type: "string",
                mode: "nullable"
              },
              %GoogleApi.BigQuery.V2.Model.TableFieldSchema{
                name: "correlation_id",
                type: "string",
                mode: "nullable"
              },
              %GoogleApi.BigQuery.V2.Model.TableFieldSchema{
                name: "entity_id",
                type: "string",
                mode: "nullable"
              },
              %GoogleApi.BigQuery.V2.Model.TableFieldSchema{
                name: "entity_type",
                type: "string",
                mode: "nullable"
              },
              %GoogleApi.BigQuery.V2.Model.TableFieldSchema{
                name: "source",
                type: "string",
                mode: "nullable"
              },
              %GoogleApi.BigQuery.V2.Model.TableFieldSchema{
                name: "action",
                type: "string",
                mode: "nullable"
              },
              %GoogleApi.BigQuery.V2.Model.TableFieldSchema{
                name: "webhook_tags",
                type: "string",
                mode: "nullable"
              },
              %GoogleApi.BigQuery.V2.Model.TableFieldSchema{
                name: "payload_version",
                type: "string",
                mode: "nullable"
              },
              %GoogleApi.BigQuery.V2.Model.TableFieldSchema{
                name: "payload",
                type: "string",
                mode: "nullable"
              }
            ]
          }
          # currently creates a _PARTITIONTIME column which is not what we need
          # we want to be able to specify 'timestamp' column as the one on which the partiotion time is calculated
          # timePartitioning: %GoogleApi.BigQuery.V2.Model.TimePartitioning{type: "DAY"}
        }
      )

    case status do
      :ok ->
        Logger.info("\ntable #{table} created\n")

      :error ->
        Logger.info("\ntable #{table} NOT created\n")

        %Tesla.Env{
          __client__: _,
          body: body,
          headers: _,
          method: _,
          opts: _,
          query: _,
          status: status,
          url: _
        } = response

        case status do
          409 -> Logger.info("\ntable #{table} already exists\n")
          400 -> Logger.info("\nbad input: #{inspect(body)}\n")
          _ -> Logger.info("\nerror. status: #{status}, body: #{inspect(body)}\n")
        end
    end

    state
  end

  defp compose_sql(state) do
    Logger.info("\ncompose_sql state in: #{inspect(state)}\n")
    rmq_payload_map = Poison.decode!(state.rmq_payload)
    fields = Map.keys(rmq_payload_map) |> list_into_parens()
    values = rmq_payload_map |> values_to_list()

    sql =
      "INSERT INTO #{state.bigquery_dataset}.#{state.bigquery_table_name} (#{fields}) VALUES #{
        values
      }"

    Logger.info("\ncompose_sql sql: #{inspect(sql)}\n")
    %{state | bigquery_sql: sql}
  end

  def values_to_list(payload) do
    values =
      Enum.reduce(payload, "(", fn {k, v}, acc ->
        case k do
          "payload" ->
            ~s(#{acc}#{","}#{inspect(Poison.encode!(v))})

          _ ->
            ~s(#{acc}#{","}"#{v}")
        end
      end)

    ~s(#{values}#{")"}) |> String.replace("(,", "(")
  end

  # defp compose_sql_old state do
  #  IO.puts "\ncompose_sql state in: #{inspect state}\n"
  #  values      = compile_value_list(state.bigquery_table_fields, Poison.decode!(state.rmq_payload))
  #  fields      = compile_field_list(state.bigquery_table_fields)
  #  sql         = "INSERT INTO #{state.bigquery_dataset}.#{state.bigquery_table_name} (#{fields}) VALUES #{values}"
  #  IO.puts "\ncompose_sql sql: #{inspect sql}\n"
  #  %{state| bigquery_sql: sql}
  # end

  # def compile_value_list(field_list,rmq_payload_map) do
  #  values = Enum.reduce(field_list,"(",fn(field,acc) -> 
  #    case field do
  #      "payload" ->
  #        IO.puts ~s(#{acc}#{Poison.encode!(Map.get(rmq_payload_map,field))})
  #        payload_map = (Map.get(rmq_payload_map,field))
  #        IO.puts "\npayload_map: #{inspect payload_map}\n"
  #        payload_encoded = Poison.encode!(payload_map)
  #        IO.puts "\npaload_encoded: #{inspect payload_encoded}\n"
  #        ~s(#{acc}#{","}#{inspect Poison.encode!(Map.get(rmq_payload_map,field))})
  #      _ ->
  #        #~s(#{acc}"#{[Map.get(rmq_payload_map,field)]}"#{","})
  #        ~s(#{acc}#{","}"#{[Map.get(rmq_payload_map,field)]}")
  #    end
  #  end)
  #  ~s(#{values}#{")"})
  # end

  # defp compile_field_list field_list do
  #  fields = Enum.reduce(field_list,"",fn(field,acc) ->
  #    acc <> "#{field},"
  #  end)
  #  fields |> String.slice(0..-2)
  # end

  def list_into_parens(a_list) do
    fields =
      Enum.reduce(a_list, "", fn field, acc ->
        acc <> "#{field},"
      end)

    fields |> String.slice(0..-2)
  end

  defp stream_into_table(state) do
    project_id = state.project_id
    conn = state.bigquery_conn
    dataset_id = state.bigquery_dataset
    table_id = state.bigquery_table_name
    sql = state.bigquery_sql
    payload_map1 = Poison.decode!(state.rmq_payload)
    payload_map = %{payload_map1 | "payload" => Poison.encode!(payload_map1["payload"])}
    row = %{json: payload_map}

    case GoogleApi.BigQuery.V2.Api.Tabledata.bigquery_tabledata_insert_all(
           conn,
           project_id,
           dataset_id,
           table_id,
           body: %GoogleApi.BigQuery.V2.Model.TableDataInsertAllRequest{
             ignoreUnknownValues: true,
             rows: [row]
           }
         ) do
      {:ok, response} ->
        Logger.info("\nInserted row, response: #{inspect(response)}\n")
        :ok

      {:error, info} ->
        %Tesla.Env{
          __client__: _client,
          __module__: _module_name,
          body: body,
          headers: _headers,
          method: _method,
          opts: _opts,
          query: _query,
          status: status,
          url: _url
        } = info

        case status do
          # token has probably expired, need to re initialize connection
          401 ->
            Logger.info("\nBigquery authorization failure, will re-establish connection\n")
            re_establish_connection(state.genserver_name)

          _ ->
            # IO.puts "\nERROR when executing bigquery job: #{inspect Poison.decode!(body)}\n"
            Logger.info("\nERROR when attempting to insert row: #{inspect(body)}\n")
        end

        :error
    end
  end

  def sync_query(state) do
    # Fetch access token
    # {:ok, token} = Goth.Token.for_scope("https://www.googleapis.com/auth/cloud-platform")
    # conn = GoogleApi.BigQuery.V2.Connection.new(token.token)
    project_id = state.project_id
    conn = state.bigquery_conn
    sql = state.bigquery_sql

    Logger.info("\nsql: #{sql}\n")
    # Make the API request
    # {:ok, response} = GoogleApi.BigQuery.V2.Api.Jobs.bigquery_jobs_query(
    {status, response} =
      GoogleApi.BigQuery.V2.Api.Jobs.bigquery_jobs_query(
        conn,
        project_id,
        body: %GoogleApi.BigQuery.V2.Model.QueryRequest{query: sql, useLegacySql: false}
      )

    case status do
      :ok ->
        case string_starts_with(sql) do
          "select" ->
            response.rows
            |> Enum.each(fn row ->
              row.f
              |> Enum.with_index()
              |> Enum.each(fn {cell, i} ->
                Logger.info("#{Enum.at(response.schema.fields, i).name}: #{cell.v}")
              end)
            end)

          "insert" ->
            Logger.info("bigquery insert response: #{inspect(response)}")
        end

        :ok

      :error ->
        # error response example
        # %Tesla.Env{
        #  __client__: %Tesla.Client{
        #    fun: nil, 
        #    post: [], 
        #    pre: [{Tesla.Middleware.Headers, :call, [%{"Authorization" => "Bearer ya29.c.ElrRBdw3r4N64Lbfd8VTEJfXQRTDmKSH1EkPaliBqhVpGyZCVKumjH-auSa9L4PhU_8B9GTZxhqFd3JZpUIuspbaASC9tjUFQbC2cf_lVrxpvCfnguGrEs-3REY"}]}] 
        #  }, 
        #  __module__: GoogleApi.BigQuery.V2.Connection, 
        #  body: "{\n\"error\":{\n\"errors\":[\n{\n\"domain\":\"global\",\n\"reason\":\"authError\",\n\"message\":\"Invalid Credentials\",\n\"locationType\":\"header\",\n\"location\":\"Authorization\"\n}\n],\n\"code\":401,\n\"message\":\"Invalid Credentials\"\n }\n}\n", 
        #  headers: %{"accept-ranges" => "none", "cache-control" => "private, max-age=0", "content-length" => "249", "content-type" => "application/json; charset=UTF-8", "date" => "Wed, 06 Jun 2018 10:53:32 GMT", "expires" => "Wed, 06 Jun 2018 10:53:32 GMT", "server" => "GSE", "vary" => "X-Origin", "www-authenticate" => "Bearer realm=\"https://accounts.google.com/\", error=invalid_token", "x-content-type-options" => "nosniff", "x-frame-options" => "SAMEORIGIN", "x-xss-protection" => "1; mode=block"}, 
        #  method: :post, 
        #  opts: [], 
        #  query: [], 
        #  status: 401, 
        #  url: url
        # }

        %Tesla.Env{
          __client__: _client,
          __module__: _module_name,
          body: body,
          headers: _headers,
          method: _method,
          opts: _opts,
          query: _query,
          status: status,
          url: _url
        } = response

        case status do
          # token has probably expired, need to re initialize connection
          401 ->
            Logger.info("\nBigquery authorization failure\n")
            re_establish_connection(state.genserver_name)

          _ ->
            Logger.info("\nERROR when executing bigquery job: #{inspect(Poison.decode!(body))}\n")
        end

        :error
    end
  end

  defp re_establish_connection(genserver_name) do
    GenServer.cast(genserver_name, {:disconnected})
  end

  def string_starts_with(string) do
    string
    |> String.trim_leading()
    |> String.downcase()
    |> String.split()
    |> List.first()
  end

  defp initialize_bigquery_connection(state) do
    project_id = Application.get_env(:writers, :bigquery_project_id, "como-env05")
    {:ok, token} = Goth.Token.for_scope("https://www.googleapis.com/auth/cloud-platform")
    bigquery_conn = GoogleApi.BigQuery.V2.Connection.new(token.token)

    ret_state = %{
      state
      | project_id: project_id,
        bigquery_conn: bigquery_conn,
        bigquery_token: token,
        bigquery_table_fields: @bigquery_table_fields
    }
  end

  defp initialize_pipeline(
         state = %{domain: domain, prefix: prefix, genserver_name: genserver_name}
       ) do
    channel = Utils.Rmq.get_amqp_channel()
    dataset = Application.get_env(:writers, :bigquery_dataset, "dodli")
    name = "#{prefix}#{domain}" |> String.trim()

    state1 = %State{
      status: :ok,
      genserver_name: genserver_name,
      env_prefix: prefix,
      rmq_qname: genserver_name,
      rmq_xname: name,
      rmq_channel: channel,
      bigquery_dataset: "#{prefix}#{dataset}" |> String.trim(),
      bigquery_table_name: domain
    }

    Logger.info("\ninitialize_pipeline about to return state: #{inspect(state1)}\n")
    state1
  end

  defp initialize_rmq_connection(state) do
    Logger.info("\ninitialize_rmq_connection received state: #{inspect(state)}\n")
    channel = state.rmq_channel
    qname = state.rmq_qname
    xname = state.rmq_xname
    Utils.Rmq.create_and_bind_queue(%{channel: channel, qname: qname, xname: xname})
    Utils.Rmq.register_as_consumer(%{channel: channel, qname: qname})
    Logger.info("worker will consume: #{inspect(qname)}")
    state
  end

  defp to_atom(str_or_atom) do
    case is_atom(str_or_atom) do
      true -> str_or_atom
      false -> String.to_atom(str_or_atom)
    end
  end
end
