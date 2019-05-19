# BigqueryGateway

#### Elasticsearch stuff  

List all  indices  

```bash  
$ curl -s http://es-hist-test-01.totango:9200/_cat/indices\?v
health status index               uuid                   pri rep docs.count docs.deleted store.size pri.store.size
green  open   291_2018_07_1_idx   9UVGYooMS9-_E_BpkjlqGQ   1   1          0            0       320b           160b
green  open   244_2019_03_1_idx   2xlIxf1qQ6G1mEmO5dBbPA   2   1          0            0       636b           318b
green  open   241_2018_07_1_idx   uo-tYaNvTYWg415pCJANJA   1   1          0            0       320b           160b
green  open   271_2019_04_1_idx   7x9sJHkBRwWaDamY9ZJ6NQ   2   1   11797542            0      1.2gb        640.5mb
green  open   515_2018_12_1_idx   vVlYZUKdT4KU1qWXyZreYA   2   1          0            0       640b           320b
green  open   106_2018_10_1_idx   WzDzxS0HS7eX0VqmTbxMWg   2   1       3510           65    836.9kb        418.4kb
green  open   246_2019_01_1_idx   fMCHCD_zRLKe_lcRkZESFw   2   1     220131            0     26.4mb         13.2mb
green  open   82_2018_12_1_idx    iL11jtM0S-Gb-0dOWXnJuQ   2   1   13414523            0      1.2gb        646.7mb

# And looking at the totango service  

curl -s http://es-hist-test-01.totango:9200/_cat/indices\?v | awk '{print $3, $9}' | grep 880 | sort
880_2018_04_1_idx 1gb
880_2018_05_1_idx 5.2gb
880_2018_06_1_idx 5.6gb
880_2018_07_1_idx 5.9gb
880_2018_08_1_idx 7.4gb
880_2018_09_1_idx 6.6gb
880_2018_10_1_idx 7.8gb
880_2018_11_1_idx 7.7gb
880_2018_12_1_idx 9.9gb
880_2019_01_1_idx 8gb
880_2019_02_1_idx 9gb
880_2019_03_1_idx 8.6gb
880_2019_04_1_idx 17.1gb
880_2019_05_1_idx 632.2mb
```  

dumping the historical prod cluster state  

```bash
$ 
```  

Get the mappings  

```bash  
$ curl http://es-hist-test-01.totango:9200/160_2019_02_1_idx/_mappings | json_pp
$ curl -XPOST http://es-hist-test-01.totango:9200/160_2019_02_1_idx/_search\?size\=5 > ./5_rows.json
```

#### Determining the bigquery schema from the elasticsearch mapping 

Lets's use the "products" obbject as an example  

products' elasticsearch mapping  

```javascript
"products" : {
  "properties" : {
     "all" : {
        "type" : "keyword"
     },
     "direct" : {
        "type" : "keyword"
     }
  }
}
```  

So in elasticsearch, any field can contain zero or more values (of the same type), so let's look at the data  
Get some sample data with "documents" field, to see its data structure  
```bash
$ curl -s http://es-hist-test-01.totango:9200/248_2019_05_1_idx/_search -d '{"query":{"query_string":{"fields":["products"],"query":"*"}}}'
```  

Extacting out the products object  

```javascript
"products":{"all":["Transportation","Marketing"],"direct":["Transportation","Marketing"]}
```  

so it's an array..  

The bigquery schema will be  

```javascript  
{
	"type": "record",
	"name": "products",
	"mode": "nullable",
	"fields": [{
		"type": "string",
		"name": "all",
		"mode": "repeated"
	}, {
		"type": "string",
		"name": "direct",
		"mode": "repeated"
	}]
}
```  


Grafana  

Historical production cluster  

http://seer-prod/d/000000046/elasticsearch-client?orgId=1&from=now%2Fw&to=now%2Fw&var-environment=prod&var-serviceId=All&var-component=application&var-activity=All&var-documentType=historical_account  
http://seer-prod/d/000000038/elasticsearch?orgId=1&var-env=prod&var-cluster=hist&var-host=All&from=now-24h&to=now
http://seer-prod/d/000000038/elasticsearch?panelId=7&fullscreen&orgId=1&var-env=prod&var-cluster=hist&var-host=All&from=now%2Fw&to=now%2Fw



vim newline between docs doc
```bash  
:%s/\"_source\":{/\n\r{/g
```  

#### Working with elasticsearch scroll  

```bash
λ  curl -s -XPOST http://es-hist-test-01.totango:9200/160_2019_04_1_idx/_search\?scroll\=1m -d '{ "size": 5,"query": { "range" : { "date" : { "gte" : "now-1d/d" } } } }'
{"_scroll_id":"the-scroll-id-AbC12","took":1,"timed_out":false,"_shards":{"total":2,"successful":2,"failed":0},"hits":{"total":0,"max_score":null,"hits":[{doc-a},{doc-b},{doc-c},{doc-k}]}}%
λ  curl -s -XPOST http://es-hist-test-01.totango:9200/160_2019_04_1_idx/_search\?scroll\=1m -d '{ "size": 5,"query": { "range" : { "date" : { "gte" : "now-1d/d" } } } }'
curl -s -XPOST http://es-hist-test-01.totango:9200/_search/scroll -d '{"scroll": "1m", "scroll_id":"the-scroll-id-AbC12"}'
{"_scroll_id":"the-scroll-id-AbC12","took":1,"timed_out":false,"_shards":{"total":2,"successful":2,"failed":0},"hits":{"total":0,"max_score":null,"hits":[]}}%
λ  curl -s -XPOST http://es-hist-test-01.totango:9200/_search/scroll -d '{"scroll": "1m", "scroll_id":"the-scroll-id-AbC12"}'
{"error":{"root_cause":[{"type":"search_context_missing_exception","reason":"No search context found for id [51641]"}],"type":"search_phase_execution_exception","reason":"all shards failed","phase":"query","grouped":true,"failed_shards":[{"shard":-1,"index":null,"reason":{"type":"search_context_missing_exception","reason":"No search context found for id [51641]"}}],"caused_by":{"type":"search_context_missing_exception","reason":"No search context found for id [51641]"}},"status":404}%
```

```bash  
$ mix run ./lib/bigquery_gateway/create_bq_table.exs --project "promenade-222313" --dataset "integration_hub" --table "historical" --schema "./priv/historical_schema.json" --partition-on "date" --cluster-on "service_id"
$
$ mix run ./lib/bigquery_gateway/es_2bq_copier.exs --es-endpoint "http://es-hist-test-01.totango:9200" --es-index "880_2019_05_1_idx" --es-days-back 1 --bq-project "promenade-222313" --bq-dataset "integration_hub" --bq-table "historical" --es-scroll-size 2000 --bq-batch-size 500
```  

