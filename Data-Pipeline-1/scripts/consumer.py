from elasticsearch import Elasticsearch

elastic_host = ""
elastic_username = ""
elastic_password = ""

es = Elasticsearch(hosts=elastic_host, basic_auth=(elastic_username, elastic_password), ca_certs='/tmp/ca.crt')
resp = es.search(
    index='kibana_sample_data_flights',
    body={
        "size": 0,
        "query": {
                    "bool": {
                        "must": [
                            {"range": {"timestamp": {"gte": "now-1h", "lte": "now"}}}
                        ]
                    }
        },
        "aggs": {
            "group_by_carrier": {
            "terms": {
                "field": "Carrier"
            },
            "aggs": {
                "national_flights_count": {
                "filter": {
                    "script": {
                    "script": {
                        "source": "doc['OriginCountry'].value == doc['DestCountry'].value"
                    }
                    }
                }
                },
                "international_flights_count": {
                "filter": {
                    "script": {
                    "script": {
                        "source": "doc['OriginCountry'].value != doc['DestCountry'].value"
                    }
                    }
                }
                },
                "average_ticket_price": {
                "avg": {
                    "field": "AvgTicketPrice"
                }
                },
                "average_flight_time_min":{
                "avg": {
                    "field": "FlightTimeMin"
                }
                },
                "total_distance_traveled":{
                "sum":{
                    "field": "DistanceKilometers"
                }
                },
                "cancelled_flights": {
                "filter": {
                    "term": {
                        "Cancelled": True
                    }
                    }
                
                },
                "delayed_flights": {
                "filter": {
                    "term": {
                        "FlightDelay": True
                    }
                    }
                
                },
                "total_delay_min":{
                "sum":{
                    "field": "FlightDelayMin"
                }
                },
                
            }
            }
        }
        }
    )

print(resp['aggregations'])
