from google.cloud import bigquery



# Set the GCS URI for the CSV file
#uri = "gs://<bucket_name>/<file_path>.csv"

# Set the dataset and table ID where the data will be loaded
#dataset_id = "<dataset_id>"
#table_id = "<table_id>"


def write_csv_to_bigquery(uri, dataset_id, table_id, schema_path):
    # Create a client object
    client = bigquery.Client()
    schema = client.schema_from_json(schema_path)
    # Configure the job to load data from GCS to BigQuery
    job_config = bigquery.LoadJobConfig(
        autodetect=True,
        source_format=bigquery.SourceFormat.CSV,
        schema=schema,
    )

    # Start the job to load data from GCS to BigQuery
    load_job = client.load_table_from_uri(
        uri, f"{dataset_id}.{table_id}", job_config=job_config
    )

    # Wait for the job to complete
    load_job.result()

    print(f"Loaded {load_job.output_rows} rows into BigQuery table {table_id}")

