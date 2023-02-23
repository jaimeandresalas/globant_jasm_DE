from google.cloud import bigquery
from google.cloud import storage
import io
from datetime import datetime


def backup_table(table_id):
    # Crear un cliente de BigQuery y obtener los datos de la tabla
    client = bigquery.Client()
    table = client.get_table(table_id)
    rows = client.list_rows(table)
    print(f"Rows in table {table_id}: {table.num_rows}")
    # Crear un cliente de Storage y obtener una referencia al bucket
    storage_client = storage.Client()
    print("Client created using default project: {}".format(storage_client.project))
    bucket_name = "gs://bucket1_jasm_globant/backup_avro"
    bucket = storage_client.bucket(bucket_name)
    print("Bucket: {}".format(bucket.name))

    # Crear un objeto BytesIO para almacenar los datos en formato AVRO
    output = io.BytesIO()

    # Escribir los datos en formato AVRO en el objeto BytesIO
    for row in rows:
        output.write(row.to_arrow().to_pybytes())

    # Crear el nombre del archivo a partir del nombre de la tabla y la fecha actual
    table_name = table.table_id
    current_date = datetime.today().strftime('%Y-%m-%d')
    filename = f"{current_date}_{table_name}.avro"
    print(f"Filename: {filename}")

    # Crear un archivo en el bucket y escribir los datos en formato AVRO
    blob = bucket.blob(filename)
    blob.upload_from_file(output, content_type="avro/binary")
