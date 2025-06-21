import requests
from datetime import datetime
from kafka import KafkaProducer
import uuid
from airflow import DAG
from airflow.operators.python import PythonOperator
import json

from cassandra.cluster import Cluster
import pandas as pd
from google.cloud import bigquery
from google.cloud.bigquery import SchemaField


def get_power_data(url, params, headers):
    """
    Fetches power data for a given zone using electricitymap.org API.
    """
    try:
        response = requests.get(url, params=params, headers=headers)
        return response.json()

    except:
        print(f'Response status code: {response.status_code}')
  

def send_powerdata_to_kafka ():
    url =  "https://api.electricitymap.org/v3/power-breakdown/latest"

    producer = KafkaProducer(bootstrap_servers='Ip_Adress:9092',
                            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                            key_serializer=lambda v: json.dumps(v).encode('utf-8')
                            )
    
    f = open('/opt/airflow/data/tokens.json')
    json_data = json.load(f)

    for i in json_data:
        data = get_power_data(url, i['headers'], i['params'])
        data['GetDataDateTime'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        data["id"] = str(uuid.uuid4())
        data["zoneid"] = i['id']
        producer.send('powerbreakdown',value=data)
        producer.flush()
    


def cassandra_to_bigquery():
    CASSANDRA_HOST = 'Ip_Adress'
    CASSANDRA_KEYSPACE = 'power'
    CASSANDRA_TABLE = 'powerbreakdown'

    BQ_PROJECT = 'realtime-streaming-461609'
    BQ_DATASET = 'powerds'
    BQ_TABLE = 'powerbreakdowntb'

    BQ_SCHEMA = [
        SchemaField("id", "STRING"),
        SchemaField("zoneid", "INTEGER"),
        SchemaField("zone", "STRING"),
        SchemaField("getdatadatetime", "TIMESTAMP"),
        SchemaField("cons_nuclear", "INTEGER"),
        SchemaField("cons_geothermal", "INTEGER"),
        SchemaField("cons_biomass", "INTEGER"),
        SchemaField("cons_coal", "INTEGER"),
        SchemaField("cons_solar", "INTEGER"),
        SchemaField("cons_wind", "INTEGER"),
        SchemaField("cons_hydro", "INTEGER"),
        SchemaField("cons_gas", "INTEGER"),
        SchemaField("cons_oil", "INTEGER"),
        SchemaField("cons_unknown", "INTEGER"),
        SchemaField("prod_nuclear", "INTEGER"),
        SchemaField("prod_geothermal", "INTEGER"),
        SchemaField("prod_biomass", "INTEGER"),
        SchemaField("prod_coal", "INTEGER"),
        SchemaField("prod_solar", "INTEGER"),
        SchemaField("prod_wind", "INTEGER"),
        SchemaField("prod_hydro", "INTEGER"),
        SchemaField("prod_gas", "INTEGER"),
        SchemaField("prod_oil", "INTEGER"),
        SchemaField("prod_unknown", "INTEGER"),
        SchemaField("consumption_total", "INTEGER"),
        SchemaField("production_total", "INTEGER"),
        SchemaField("import_total", "INTEGER"),
        SchemaField("export_total", "INTEGER"),
    ]

    cluster = Cluster([CASSANDRA_HOST])
    session = cluster.connect(CASSANDRA_KEYSPACE)
    query = f"SELECT * FROM {CASSANDRA_TABLE};"
    rows = session.execute(query)
    df = pd.DataFrame(rows.all(), columns=rows.column_names)
    df['id'] = df['id'].astype(str)
    cluster.shutdown()

    client = bigquery.Client(project=BQ_PROJECT)
    table_id = f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"
    
    try:
        client.get_table(table_id)
    except Exception:
        table = bigquery.Table(table_id, schema=BQ_SCHEMA)
        client.create_table(table)

    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()
    print(f"Uploaded {job.output_rows} rows to {table_id}")


default_args = {
    'start_date': datetime(2024, 1, 1)
}

with DAG(
    dag_id='etl_data__api_kafkaandcassandra_bigdata',
    schedule_interval='@daily',
    catchup=False,
    default_args=default_args,
    tags=['etl']
) as dag:

    api_kafka_etl_task = PythonOperator(
        task_id='send_powerdata_to_kafka',
        python_callable=send_powerdata_to_kafka
    )

    cassandra_bigdata_etl_task = PythonOperator(
        task_id='cassandra_to_bigquery_task',
        python_callable=cassandra_to_bigquery,
    )

    api_kafka_etl_task >> cassandra_bigdata_etl_task