from fastapi import FastAPI
from typing import List
from pydantic import BaseModel
from sql_app.models import hired_employee, departments, jobs
from sql_app.historical_data import write_csv_to_bigquery
app = FastAPI()


@app.get("/")
async def root():
    return {"message": "Hello World from FastAPI and GCP Cloud Run!"}

@app.post("/write_csv")
def write_csv():
    dataset_id = "gentle-coyote-378216.globant_de"
    # Set the GCS URI for the CSV file
    uri_departments = "gs://bucket1_jasm_globant/backup_csv/departments.csv"
    uri_jobs = "gs://bucket1_jasm_globant/backup_csv/jobs.csv"
    uri_hired_employee = "gs://bucket1_jasm_globant/backup_csv/hired_employees.csv"
    # Set the dataset and table ID where the data will be loaded
    table_id_departments = "departments"
    table_id_jobs = "jobs"
    table_id_hired_employee = "hired_employee"
    # Set the schema path for the tables 
    #schema_departments = "gs://bucket1_jasm_globant/backup_csv/schemas_json/departments.json"
    #schema_jobs = "gs://bucket1_jasm_globant/backup_csv/schemas_json/jobs.json"
    #schema_hired_employee = "gs://bucket1_jasm_globant/backup_csv/schemas_json/jobs.json"
    schema_departments = "data/schemas_json/departments.json"
    schema_jobs = "data/schemas_json/jobs.json"
    schema_hired_employee = "data/schemas_json/hired_employee.json"
    # Write the CSV files to BigQuery
    try:
        write_csv_to_bigquery(uri_departments, dataset_id, table_id_departments, schema_departments)
        write_csv_to_bigquery(uri_jobs, dataset_id, table_id_jobs, schema_jobs)
        write_csv_to_bigquery(uri_hired_employee, dataset_id, table_id_hired_employee, schema_hired_employee)
    except Exception as e:
        print(e)
        return {"message": "Error inserting CVS files to BigQuery"}
    return {"message": "Transactions inserted CVS files successfully"}




