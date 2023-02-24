


number_employees = """
with a as (
select 
*, date(PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%SZ', datetime)) as date,

FROM gentle-coyote-378216.globant_de.hired_employee)
select d.department, j.job,
COUNT(CASE WHEN a.date > '2021-01-01' and a.date < '2021-04-01' THEN 1 END) AS Q1,
COUNT(CASE WHEN a.date > '2021-04-01' and a.date < '2021-07-01' THEN 1 END) AS Q2,
COUNT(CASE WHEN a.date > '2021-07-01' and a.date < '2021-10-01' THEN 1 END) AS Q3,
COUNT(CASE WHEN a.date > '2021-10-01' and a.date < '2022-01-01' THEN 1 END) AS Q4,
from a
JOIN gentle-coyote-378216.globant_de.departments d ON a.department_id = d.id
JOIN gentle-coyote-378216.globant_de.jobs j ON a.job_id = j.id
GROUP BY d.department, j.job
ORDER BY d.department, j.job
"""


hired_more_than_mean = """
    with a as (
select 
*, date(PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%SZ', datetime)) as date,

FROM gentle-coyote-378216.globant_de.hired_employee)


SELECT d.id AS department_id, d.department AS department_name, COUNT(*) AS num_employees_hired
FROM a
JOIN gentle-coyote-378216.globant_de.departments d ON a.department_id = d.id
JOIN gentle-coyote-378216.globant_de.jobs j ON a.job_id = j.id
WHERE EXTRACT(year from date) = 2021
GROUP BY department_id, department_name
HAVING COUNT(*) > (SELECT AVG(num_employees_hired) FROM (
  SELECT d.id AS department_id, COUNT(*) AS num_employees_hired
  FROM a
  JOIN gentle-coyote-378216.globant_de.departments d ON a.department_id = d.id
  WHERE extract(YEAR from date) = 2021
  GROUP BY department_id
))
ORDER BY num_employees_hired DESC

"""