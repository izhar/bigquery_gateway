# Running historical reports on the data  

#### the big services  

- Monster - 25291
- Egnyte - 25390
- Rave Mobile - 25159
- DaySmart - 8502  

#### Elasticsearch  

Totango DSL call  

```bash
curl -X POST \
  https://app.totango.com/api/v2/aggregations/accounts/historical \
  -H 'Cache-Control: no-cache' \
  -H 'Content-Type: application/x-www-form-urlencoded' \
  -H 'app-token: faa929ef269868fcdd424201bae6fab09f88419eAharon@totango.com' \
  -d 'segments={"all":[]}&aggregations={"my_cool_agg": {"aggregator":"document_field.date","interval":"day","min_bounds":"2019-01-02","max_bounds":"2019-05-12","sub_aggregations":{"great health stuff":{"aggregator":"health.health","aggregations":{"activity stuff":"named_aggregations.activity_days.14_days","cv stuff":"attributes.Contract Value"}},"great support_type stuff":{"aggregator":"attributes.support_type","aggregations":{"support_type stats":"named_aggregations.activity_days.14_days","support_type cv stats":"attributes.Contract Value"}}}}}&concise=true'
```  

Resulting Elasticsearch call  

```javascript
{
  "size" : 0,
  "query" : {
    "bool" : {
      "filter" : [
        {
          "term" : {
            "service_id" : {
              "value" : "880",
              "boost" : 1.0
            }
          }
        },
        {
          "bool" : {
            "must" : [
              {
                "bool" : {
                  "must_not" : [
                    {
                      "nested" : {
                        "query" : {
                          "term" : {
                            "date_attributes.key" : {
                              "value" : "Deletion candidate",
                              "boost" : 1.0
                            }
                          }
                        },
                        "path" : "date_attributes",
                        "ignore_unmapped" : false,
                        "score_mode" : "none",
                        "boost" : 1.0
                      }
                    }
                  ],
                  "disable_coord" : false,
                  "adjust_pure_negative" : true,
                  "boost" : 1.0
                }
              },
              {
                "bool" : {
                  "must" : [
                    {
                      "range" : {
                        "date" : {
                          "from" : 1546416000000,
                          "to" : 1557734399999,
                          "include_lower" : true,
                          "include_upper" : true,
                          "boost" : 1.0
                        }
                      }
                    }
                  ],
                  "disable_coord" : false,
                  "adjust_pure_negative" : true,
                  "boost" : 1.0
                }
              }
            ],
            "disable_coord" : false,
            "adjust_pure_negative" : true,
            "boost" : 1.0
          }
        }
      ],
      "disable_coord" : false,
      "adjust_pure_negative" : true,
      "boost" : 1.0
    }
  },
  "aggregations" : {
    "my_cool_agg__KEY#_1" : {
      "date_histogram" : {
        "field" : "date",
        "format" : "yyyy-MM-dd",
        "time_zone" : "-08:00",
        "interval" : "1d",
        "offset" : 0,
        "order" : {
          "_key" : "asc"
        },
        "keyed" : false,
        "min_doc_count" : 0,
        "extended_bounds" : {
          "min" : 1546416000000,
          "max" : 1557648000000
        }
      },
      "aggregations" : {
        "great health stuff__KEY#_2" : {
          "terms" : {
            "field" : "health_data.health",
            "size" : 2147483647,
            "min_doc_count" : 1,
            "shard_min_doc_count" : 0,
            "show_term_doc_count_error" : false,
            "order" : [
              {
                "_count" : "desc"
              },
              {
                "_term" : "asc"
              }
            ]
          },
          "aggregations" : {
            "nest_filter_cv stuff__KEY#_5" : {
              "nested" : {
                "path" : "number_attributes"
              },
              "aggregations" : {
                "filter_cv stuff__KEY#_5" : {
                  "filter" : {
                    "term" : {
                      "number_attributes.key" : {
                        "value" : "Contract Value",
                        "boost" : 1.0
                      }
                    }
                  },
                  "aggregations" : {
                    "cv stuff__KEY#_5" : {
                      "stats" : {
                        "field" : "number_attributes.value"
                      }
                    }
                  }
                }
              }
            },
            "nest_filter_activity stuff__KEY#_4" : {
              "nested" : {
                "path" : "named_aggregations"
              },
              "aggregations" : {
                "filter_activity stuff__KEY#_4" : {
                  "filter" : {
                    "term" : {
                      "named_aggregations.name" : {
                        "value" : "activity_days",
                        "boost" : 1.0
                      }
                    }
                  },
                  "aggregations" : {
                    "activity stuff__KEY#_4" : {
                      "stats" : {
                        "field" : "named_aggregations.value14"
                      }
                    }
                  }
                }
              }
            }
          }
        },
        "nest_filter_great support_type stuff__KEY#_3" : {
          "nested" : {
            "path" : "string_attributes"
          },
          "aggregations" : {
            "filter_great support_type stuff__KEY#_3" : {
              "filter" : {
                "term" : {
                  "string_attributes.key" : {
                    "value" : "support_type",
                    "boost" : 1.0
                  }
                }
              },
              "aggregations" : {
                "great support_type stuff__KEY#_3" : {
                  "terms" : {
                    "field" : "string_attributes.value",
                    "size" : 2147483647,
                    "min_doc_count" : 1,
                    "shard_min_doc_count" : 0,
                    "show_term_doc_count_error" : false,
                    "order" : [
                      {
                        "_count" : "desc"
                      },
                      {
                        "_term" : "asc"
                      }
                    ]
                  },
                  "aggregations" : {
                    "reverse_nested_nest_filter_support_type stats__KEY#_6" : {
                      "reverse_nested" : { },
                      "aggregations" : {
                        "nest_filter_support_type stats__KEY#_6" : {
                          "nested" : {
                            "path" : "named_aggregations"
                          },
                          "aggregations" : {
                            "filter_support_type stats__KEY#_6" : {
                              "filter" : {
                                "term" : {
                                  "named_aggregations.name" : {
                                    "value" : "activity_days",
                                    "boost" : 1.0
                                  }
                                }
                              },
                              "aggregations" : {
                                "support_type stats__KEY#_6" : {
                                  "stats" : {
                                    "field" : "named_aggregations.value14"
                                  }
                                }
                              }
                            }
                          }
                        }
                      }
                    },
                    "reverse_nested_nest_filter_support_type cv stats__KEY#_7" : {
                      "reverse_nested" : { },
                      "aggregations" : {
                        "nest_filter_support_type cv stats__KEY#_7" : {
                          "nested" : {
                            "path" : "number_attributes"
                          },
                          "aggregations" : {
                            "filter_support_type cv stats__KEY#_7" : {
                              "filter" : {
                                "term" : {
                                  "number_attributes.key" : {
                                    "value" : "Contract Value",
                                    "boost" : 1.0
                                  }
                                }
                              },
                              "aggregations" : {
                                "support_type cv stats__KEY#_7" : {
                                  "stats" : {
                                    "field" : "number_attributes.value"
                                  }
                                }
                              }
                            }
                          }
                        }
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
  }
}
```  


