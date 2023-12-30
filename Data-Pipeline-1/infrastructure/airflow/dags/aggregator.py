import os
import pandas as pd
from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task

from elasticsearch import Elasticsearch

from sqlalchemy import create_engine



def connect_to_elastic():
    host = os.getenv('ELASTIC_HOST')
    port = os.getenv('ELASTIC_PORT')
    username = os.getenv('ELASTIC_USER')
    password = os.getenv('ELASTIC_PASSWORD')

    return Elasticsearch(hosts=f"https://{host}:{port}", basic_auth=(username, password), verify_certs=False)

def connect_to_postgres():
    host = os.getenv('POSTGRES_HOST')
    port = os.getenv('POSTGRES_PORT')
    username = os.getenv('POSTGRES_USER')
    password = os.getenv('POSTGRES_PASSWORD')
    db_name = os.getenv('POSTGRES_DB_NAME')

    return create_engine(f"postgresql://{username}:{password}@{host}:{port}/{db_name}")

def query_elastic(es):
    query = {
        "size": 0,
        "query": {
                    "bool": {
                        "must": [
                            {"range": {"timestamp": {"gte": "now-24h", "lte": "now"}}}
                        ]
                    }
        },
        "aggs": {
            "group_by_carrier": {
            "terms": {
                "field": "Carrier"
            },
            "aggs": {
                "national_flights": {
                "filter": {
                    "script": {
                    "script": {
                        "source": "doc['OriginCountry'].value == doc['DestCountry'].value"
                    }
                    }
                }
                },
                "international_flights": {
                "filter": {
                    "script": {
                    "script": {
                        "source": "doc['OriginCountry'].value != doc['DestCountry'].value"
                    }
                    }
                }
                },
                "avg_ticket_price_dollars": {
                "avg": {
                    "field": "AvgTicketPrice"
                }
                },
                "avg_flight_time_hours":{
                "avg": {
                    "field": "FlightTimeMin"
                }
                },
                "avg_distance_traveled_km":{
                "avg":{
                    "field": "DistanceKilometers"
                }
                },
                "total_distance_traveled_km":{
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
                "avg_delay_hours":{
                "avg":{
                    "field": "FlightDelayMin"
                }
                },
                
            }
            }
        }
        }
    
    return es.search(index='kibana_sample_data_flights', body=query)
    
@task(task_id='aggregator')
def aggregator():
    # establish connections
    es = connect_to_elastic()
    engine = connect_to_postgres()

    # get results from query
    resp = query_elastic(es)
    
    # main table
    data = pd.DataFrame(columns=['carrier', 'date', 'national_flights', 'international_flights', 'avg_ticket_price_dollars', 'avg_flight_time_hours', 'avg_distance_traveled_km', 'total_distance_traveled_km', 'cancelled_flights', 'delayed_flights', 'avg_delay_hours'])

    # extract data from json response for each carrier and append to main table
    for response in resp['aggregations']["group_by_carrier"]["buckets"]:
        df = pd.json_normalize(response)

        # drop and rename columns
        df.drop('doc_count', axis=1, inplace=True)
        df.rename(columns={'key': 'carrier'}, inplace=True)
        df.columns = df.columns.str.replace('.doc_count', '')
        df.columns = df.columns.str.replace('.value', '')

        # add empty date colum
        df['date'] = None
        # reorder columns
        df = df[['carrier', 'date', 'national_flights', 'international_flights', 'avg_ticket_price_dollars', 'avg_flight_time_hours', 'avg_distance_traveled_km', 'total_distance_traveled_km', 'cancelled_flights', 'delayed_flights', 'avg_delay_hours']]

        # append 
        data.loc[len(data)] = df.values[0]

    # add yesterdays date (calculations happen at midnight so we need to add date from the past 24 hours)
    data['date'] = datetime.today().strftime('%Y-%m-%d')

    # minutes to hours (elastic data is available only in minutes format)
    data['avg_flight_time_hours'] = data['avg_flight_time_hours'] / 60
    data['avg_delay_hours'] = data['avg_delay_hours'] / 60

    # write to Postgres
    with engine.connect() as connection:
        data.to_sql(
            name='flight_data',
            con=engine,
            if_exists='append',
            index=False
        )

with DAG(
    dag_id='elastic_aggregator_dag',
    schedule='@daily',
    start_date=datetime(2023, 12, 30),
    description='Produce aggregations on elastic flight data',
    default_args={'retries': 1},
    catchup=False,
    dagrun_timeout=timedelta(hours=1)
) as dag:
    
    task = aggregator()
