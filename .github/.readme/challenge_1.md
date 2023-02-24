# Challenge 1 This project is big data migration to a new database system. You need to create a PoC to solve the next requirements:
1. Move historic data from files in CSV format to the new database.
2. Create a Rest API service to receive new data. This service must have:
    2.1. Each new transaction must fit the data dictionary rules.
    2.2. Be able to insert batch transactions (1 up to 1000 rows) with one request. 
    2.3. Receive the data for each table in the same service.
    2.4. Keep in mind the data rules for each table.
3. Create a feature to backup for each table and save it in the file system in AVRO format.
4. Create a feature to restore a certain table with its backup.

## Solution: 
    1. Create Api with Fast Api 
    2. Using BigQuery, GCS and Cloud Run for create this solution
    3. Using Bigquery as warehouse and using python api for read, create and put new data in BigQuery.
    4. Run all the solution in Cloud Run using docker technologies
    5. Creating a CI/CD with Github Actions
    