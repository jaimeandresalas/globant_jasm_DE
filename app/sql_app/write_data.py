from google.cloud import bigquery
from sql_app.models import Jobs, Departments, HiredEmployee
from typing import List
from pydantic import ValidationError
import pandas as pd

def write_table(data: List[dict], table_name: str, dataset_id : str):
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

