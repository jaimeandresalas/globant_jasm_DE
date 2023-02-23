from google.cloud import bigquery
from google.cloud import storage
import io
import fastavro
from datetime import datetime
import pandas as pd


def upload_blob(bucket_name, source_bytes, destination_blob_name):
    """Sube un objeto de BytesIO a un bucket de GCS."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_string(source_bytes.getvalue(), content_type='avro/binary')
    print(f'Archivo {destination_blob_name} subido a {bucket_name}.')

def backup_table(table_id):
    # Crear un cliente de BigQuery y obtener los datos de la tabla
    client = bigquery.Client()
    table = client.get_table(table_id)
    rows = client.list_rows(table)
    #rows_2 = rows.to_arrow()
    df = rows.to_dataframe()

    # Crear un cliente de Storage y obtener una referencia al bucket
    bucket_name = "gs://bucket1_jasm_globant/backup_avro"
    


    # Crear un objeto BytesIO para almacenar los datos en formato AVRO
    output = io.BytesIO()
    # Crear el esquema de la tabla
    schema = table.schema.to_json()
    # Escribir los datos en el objeto BytesIO
    fastavro.writer(output, schema, df.to_dict(orient='records'))
    #obtener los datos en bytes
    avro_bytes = output.getvalue()

    # Crear el nombre del archivo a partir del nombre de la tabla y la fecha actual
    table_name = table_id.split(".")[-1]
    current_date = datetime.today().strftime('%Y-%m-%d')
    filename = f"{current_date}_{table_name}.avro"
    upload_blob(bucket_name, avro_bytes, filename)
