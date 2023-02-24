from google.cloud import bigquery

def write_csv_to_bigquery(uri, dataset_id, table_id, schema_path):
    """
    It loads a CSV file from GCS to BigQuery
    
    :param uri: The URI of the CSV file in GCS
    :param dataset_id: The name of the dataset you want to create
    :param table_id: The name of the table you want to create in BigQuery
    :param schema_path: The path to the JSON schema file
    """
    # Create a client object
    client = bigquery.Client()
    schema = client.schema_from_json(schema_path)
    # Configure the job to load data from GCS to BigQuery
    job_config = bigquery.LoadJobConfig(
        autodetect=True,
        source_format=bigquery.SourceFormat.CSV,
        schema=schema,
        max_bad_records=1000,
        ignore_unknown_values=False,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

    # Start the job to load data from GCS to BigQuery
    load_job = client.load_table_from_uri(
        uri, f"{dataset_id}.{table_id}", job_config=job_config
    )

    # Wait for the job to complete
    load_job.result()
    # Verificamos si hubo errores en la carga
    if load_job.errors:
        print(f'Errores en la carga de datos a la tabla f"{dataset_id}.{table_id}":')
        for error in load_job.errors:
            print(error)
    print(f"Loaded {load_job.output_rows} rows into BigQuery table {table_id}")

