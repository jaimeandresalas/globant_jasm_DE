from google.cloud import bigquery 
from datetime import datetime

def export_table_to_avro(table_id, bucket_name):
    client = bigquery.Client()
    #dataset_ref = client.dataset(dataset_id, project=project)
    #table_ref = dataset_ref.table(table_id)
    table_ref = table_id
    print(table_ref)

    job_config = bigquery.job.ExtractJobConfig()
    job_config.destination_format = bigquery.DestinationFormat.AVRO
    table_name = table_id.split(".")[-1]
    current_date = datetime.today().strftime('%Y-%m-%d')
    filename = f"{current_date}_{table_name}.avro"
    print(filename)
    destination_uri = 'gs://{}/{}'.format(bucket_name, filename)
    print(destination_uri)
    extract_job = client.extract_table(
        table_ref,
        destination_uri,
        job_config=job_config,
        location="EU",
        )  
    extract_job.result()    
    print('Exported {} to {}'.format(table_id, destination_uri))