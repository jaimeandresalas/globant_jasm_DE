from pydantic import BaseModel

class HiredEmployee(BaseModel):
    id: int
    name: str
    datetime: str
    department_id : int
    job_id : int

class Departments(BaseModel):
    id: int
    departament: str

class Jobs(BaseModel):
    id: int
    job: str