
from cassandra.cluster import Cluster
import pandas as pd
from google.cloud import bigquery
from google.cloud.bigquery import SchemaField
from datetime import datetime


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
    client.get_table(table_id)  # Check if table exists
    print(f"BigQuery table {table_id} already exists.")
except Exception:
    print(f"Creating BigQuery table {table_id}...")
    table = bigquery.Table(table_id, schema=BQ_SCHEMA)
    client.create_table(table)
    print("Table created.")


job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",  # Overwrite table
    )

job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
job.result()
print(f"Uploaded {job.output_rows} rows to {table_id}")



