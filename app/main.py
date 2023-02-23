from fastapi import FastAPI
from typing import List
from sql_app.historical_data import write_csv_to_bigquery
#from sql_app.write_data import write_table
from sql_app.models import Jobs, Departments, HiredEmployee
from pydantic import ValidationError
from google.cloud import bigquery
import pandas as pd
from sql_app.backup_avro import export_table_to_avro
from datetime import datetime

app = FastAPI()


@app.get("/")
async def root():
    return {"message": "Hello World from FastAPI and GCP Cloud Run!"}

@app.post("/write_csv")
async def write_csv():
    dataset_id = "gentle-coyote-378216.globant_de"
    # Set the GCS URI for the CSV file
    uri_departments = "gs://bucket1_jasm_globant/backup_csv/departments.csv"
    uri_jobs = "gs://bucket1_jasm_globant/backup_csv/jobs.csv"
    uri_hired_employee = "gs://bucket1_jasm_globant/backup_csv/hired_employees.csv"
    # Set the dataset and table ID where the data will be loaded
    table_id_departments = "departments"
    table_id_jobs = "jobs"
    table_id_hired_employee = "hired_employee"
    # Set the schema path for the tables to be created
    schema_departments = "data/schemas_json/departments.json"
    schema_jobs = "data/schemas_json/jobs.json"
    schema_hired_employee = "data/schemas_json/hired_employees.json"
    # Write the CSV files to BigQuery
    try:
        write_csv_to_bigquery(uri_departments, dataset_id, table_id_departments, schema_departments)
        print("Departments inserted successfully")
        write_csv_to_bigquery(uri_jobs, dataset_id, table_id_jobs, schema_jobs)
        print("Jobs inserted successfully")
        write_csv_to_bigquery(uri_hired_employee, dataset_id, table_id_hired_employee, schema_hired_employee)
        print("Hired Employee inserted successfully")
    except Exception as e:
        print(e)
        return {"message": "Error inserting CVS files to BigQuery"}
    return {"message": "Transactions inserted CVS files successfully"}



# Ruta para insertar datos en las tablas
@app.post("/insert_data")
async def write_table(data: List[dict], table_name: str):
    dataset_id = "gentle-coyote-378216.globant_de"
    client = bigquery.Client()
    # Convertir los datos recibidos a objetos Pydantic
    if table_name == "jobs":
        SchemaClass = Jobs
        table_id = f"{dataset_id}.{table_name}"
       # schema_path = "data/schemas_json/jobs.json"
    elif table_name == "departments":
        SchemaClass = Departments
        table_id = f"{dataset_id}.{table_name}"
        #schema_path = "data/schemas_json/departments.json"
    elif table_name == "hired_employee":
        SchemaClass = HiredEmployee
        table_id = f"{dataset_id}.{table_name}"
        #schema_path = "data/schemas_json/hired_employees.json"
    else:
        return {"error": "Invalid table name"}

    try:
        data_list = [SchemaClass(**d) for d in data]
    except ValidationError as e:
        error_msgs = []
        for error in e.errors():
            error_msgs.append(f"El campo {error['loc']} no cumple con el formato: {error['msg']}")
        return {"error": error_msgs}
    job_config = bigquery.LoadJobConfig(
        autodetect=True,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND
    )
    df=pd.DataFrame([d.dict() for d in data_list])

    # Insertar los datos en BigQuery
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()  # Esperar a que termine el trabajo
    
    # Verificamos si hubo errores en la carga
    if job.errors:
        print(f'Errores en la carga de datos a la tabla "{table_id}":')
        for error in job.errors:
            print(error)
    
    print(f"Loaded {job.output_rows} rows into BigQuery table {table_id}")


@app.post("/backup_avro")
async def backup_avro(table_name :str):
    dataset_id = "gentle-coyote-378216.globant_de"
    bucket_name = "gs://bucket1_jasm_globant/backup_avro"
    if table_name == "jobs":
        table_id = f"{dataset_id}.{table_name}"
    elif table_name == "departments":
        table_id = f"{dataset_id}.{table_name}"
    elif table_name == "hired_employee":
        table_id = f"{dataset_id}.{table_name}"
    else:
        return {"error": "Invalid table name"}
    try: 
        print("Exporting table to avro")
        print(table_id)
        client = bigquery.Client()
        dataset_id = "globant-de"
        project = "gentle-coyote-378216"
        dataset_ref = client.dataset(dataset_id, project=project)
        table_id = "jobs"
        table_ref = dataset_ref.table(table_id)
        #table_ref = table_id
        #table_ref = "gentle-coyote-378216.globant_de.jobs"
        print(table_ref)
        job_config = bigquery.job.ExtractJobConfig()
        job_config.destination_format = bigquery.DestinationFormat.AVRO
        table_name = table_id.split(".")[-1]
        current_date = datetime.today().strftime('%Y-%m-%d')
        filename = f"{current_date}_{table_name}.avro"
        print(filename)
        destination_uri = '{}/{}'.format(bucket_name, filename)
        print(destination_uri)
        extract_job = client.extract_table(
            table_ref,
            destination_uri,
            job_config=job_config,
            location="EU",
            )  
        extract_job.result()    
        print('Exported {} to {}'.format(table_id, destination_uri))
        
        #print()
        #export_table_to_avro(table_id,bucket_name)
    except Exception as e:
        print(e)
        return {"message": "Error backing up table"}
    return {"message": "Table backed up successfully"}
