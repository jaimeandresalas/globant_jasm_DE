from pydantic import BaseModel

class hired_employee(BaseModel):
    id: int
    name: str
    datetime: str
    department_id : int
    job_id : int

class departments(BaseModel):
    id: int
    departament: str

class jobs(BaseModel):
    id: int
    job: str