from fastapi import FastAPI, HTTPException
from typing import List
from sql_app.historical_data import write_csv_to_bigquery
from sql_app.models import Jobs, Departments, HiredEmployee
from pydantic import ValidationError
from google.cloud import bigquery
import pandas as pd
from datetime import datetime
from sql_app.queries import number_employees, hired_more_than_mean


app = FastAPI()


@app.get("/")
async def root():
    return {"message": "Hello World from FastAPI and GCP Cloud Run!"}

@app.post("/write_csv")
async def write_csv():
    """
    It takes the CSV files from the GCS bucket and inserts them into the BigQuery tables
    :return: a message with the result of the transaction.
    """
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
        return {"message": "Error inserting CVS files to BigQuery" },401
    return {"message": "Transactions inserted CVS files successfully"}



# Ruta para insertar datos en las tablas
@app.post("/insert_data")
async def write_table(data: List[dict], table_name: str):
    """
    It takes a list of dictionaries and a table name, validates the data, and inserts it into BigQuery
    
    :param data: List[dict]
    :type data: List[dict]
    :param table_name: The name of the table to write to
    :type table_name: str
    :return: A dictionary with a message that says that the data was inserted successfully.
    """
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
        return {"error": "Invalid table name"},402

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
    return {"message": "Data inserted successfully"}


@app.post("/backup_avro")
async def backup_avro(table_name :str):
    """
    It takes a table name as an argument, and exports the table to a bucket in Avro format
    
    :param table_name: The name of the table to be backed up
    :type table_name: str
    :return: A dictionary with a message
    """
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

    client = bigquery.Client()
    job_config = bigquery.job.ExtractJobConfig()
    job_config.destination_format = bigquery.DestinationFormat.AVRO
    table_name = table_id.split(".")[-1]
    current_date = datetime.today().strftime('%Y-%m-%d')
    filename = f"{current_date}_{table_name}.avro"
    destination_uri = '{}/{}'.format(bucket_name, filename)
    table_ref = table_id
    print(table_ref)
    extract_job = client.extract_table(
        table_ref,
        destination_uri,
        job_config=job_config,
        )  
    extract_job.result()
    if extract_job.errors:
        print(f'Errores en la carga de datos a la tabla "{table_id}":')
    print('Exported {} to {}'.format(table_id, destination_uri))
    return {"message": "Table backed up successfully"}

@app.post("/write_avro")
async def write_avro(table_name : str, backup_name : str):
    """
    It takes a table name and a backup name as parameters, and then it loads the backup Avro file from the
    bucket into the table in BigQuery
    
    :param table_name: The name of the table you want to backup
    :type table_name: str
    :param backup_name: The name of the backup file
    :type backup_name: str
    :return: A dictionary with a message key and a value.
    """
    dataset_id = "gentle-coyote-378216.globant_de"
    bucket_name = "gs://bucket1_jasm_globant/backup_avro"
    if table_name == "jobs":
        schema_path = "data/schemas_json/jobs.json"
        table_id = f"{dataset_id}.{table_name}"
    elif table_name == "departments":
        schema_path = "data/schemas_json/departments.json"
        table_id = f"{dataset_id}.{table_name}"
    elif table_name == "hired_employee":
        schema_path = "data/schemas_json/hired_employees.json"
        table_id = f"{dataset_id}.{table_name}"
    else:
        return {"error": "Invalid table name"},402
    # Construct a BigQuery client object.
    client = bigquery.Client()

    #job_config = bigquery.LoadJobConfig(source_format=bigquery.SourceFormat.AVRO)
    schema = client.schema_from_json(schema_path)
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.AVRO,
        schema=schema,
        max_bad_records=1000,
        ignore_unknown_values=False,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    #uri = "gs://cloud-samples-data/bigquery/us-states/us-states.avro"
    print("Bucket name")
    print(backup_name)
    uri = "gs://bucket1_jasm_globant/backup_avro/"+backup_name
    print(uri)

    load_job = client.load_table_from_uri(
        uri, table_id, job_config=job_config
    )  # Make an API request.

    try:
        load_job.result()  # Waits for the job to complete.

        destination_table = client.get_table(table_id)
        print("Loaded {} rows.".format(destination_table.num_rows))
        
    except Exception as e:
        print("Error: {}".format(e))
        return {"message": "Error inserting Avro files to BigQuery" },401
    
    return {"message": "Table backed up successfully"}


@app.get("/number_employee")
async def get_table_numberemployee():
    """
    It takes the query string, and returns the results of the query as a list of dictionaries
    :return: The number of employees hired in each year.
    """
    client = bigquery.Client()
    try:
        query_job = client.query(number_employees)
        results = query_job.result()
        return [dict(row) for row in results]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/hired_more_than_mean")
async def get_table_hired_more_than_mean():
    """
    It creates a BigQuery client, runs the query, and returns the results
    :return: A list of dictionaries.
    """
    client = bigquery.Client()
    try:
        query_job = client.query(hired_more_than_mean)
        results = query_job.result()
        return [dict(row) for row in results]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))