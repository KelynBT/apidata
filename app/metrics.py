from typing import List, Dict, Any
from sqlalchemy import text
from .db import engine

def hires_by_quarter(year: int) -> List[Dict[str, Any]]:
    sql = text("""
        WITH base AS (
          SELECT d.name AS department, j.name AS job,
                 EXTRACT(QUARTER FROM e.dt)::int AS qtr
          FROM app.employees e
          JOIN app.departments d ON d.id = e.department_id
          JOIN app.jobs j        ON j.id = e.job_id
          WHERE EXTRACT(YEAR FROM e.dt) = :yr
        )
        SELECT department, job,
               SUM(CASE WHEN qtr=1 THEN 1 ELSE 0 END) AS q1,
               SUM(CASE WHEN qtr=2 THEN 1 ELSE 0 END) AS q2,
               SUM(CASE WHEN qtr=3 THEN 1 ELSE 0 END) AS q3,
               SUM(CASE WHEN qtr=4 THEN 1 ELSE 0 END) AS q4
        FROM base
        GROUP BY department, job
        ORDER BY department ASC, job ASC
    """)
    with engine.begin() as conn:
        return [dict(r._mapping) for r in conn.execute(sql, {"yr": year})]

def departments_above_avg(year: int) -> List[Dict[str, Any]]:
    sql = text("""
        WITH per_dept AS (
          SELECT e.department_id AS id, COUNT(*) AS hired
          FROM app.employees e
          WHERE EXTRACT(YEAR FROM e.dt) = :yr
          GROUP BY e.department_id
        ), avg_all AS (
          SELECT AVG(hired)::numeric AS avg_hired FROM per_dept
        )
        SELECT d.id, d.name AS department, p.hired
        FROM per_dept p
        JOIN app.departments d ON d.id = p.id
        CROSS JOIN avg_all a
        WHERE p.hired > a.avg_hired
        ORDER BY p.hired DESC, d.name ASC
    """)
    with engine.begin() as conn:
        return [dict(r._mapping) for r in conn.execute(sql, {"yr": year})]
