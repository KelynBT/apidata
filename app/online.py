# app/online.py
from typing import List, Dict, Any, Literal
from fastapi import HTTPException
from sqlalchemy import text
from .db import engine
from .ingest import _parse_dt  # ya existe

TableName = Literal["departments", "jobs", "employees"]

def _validate_departments(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    good, bad = [], []
    for r in rows:
        try:
            rid = int(r["id"])
            name = (r["name"] or "").strip()
            if not name:
                raise ValueError("missing name")
            good.append({"id": rid, "name": name})
        except Exception as e:
            r["_reason"] = str(e); bad.append(r)
    if bad:
        raise HTTPException(400, detail={"invalid_rows": bad})
    return good

def _validate_jobs(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return _validate_departments(rows)

def _validate_employees(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    with engine.begin() as conn:
        dept_ids = {row[0] for row in conn.execute(text("SELECT id FROM app.departments"))}
        job_ids  = {row[0] for row in conn.execute(text("SELECT id FROM app.jobs"))}

    good, bad = [], []
    for r in rows:
        try:
            rid  = int(r["id"])
            name = (r["name"] or "").strip()
            dt   = _parse_dt(r.get("datetime") or r.get("dt"))
            dep  = int(r["department_id"])
            job  = int(r["job_id"])
            if not name or dt is None:
                raise ValueError("missing/invalid name or datetime")
            if dep not in dept_ids: raise ValueError(f"department {dep} not found")
            if job not in job_ids:  raise ValueError(f"job {job} not found")
            good.append({"id": rid, "name": name, "dt": dt, "department_id": dep, "job_id": job})
        except Exception as e:
            r["_reason"] = str(e); bad.append(r)
    if bad:
        raise HTTPException(400, detail={"invalid_rows": bad})
    return good

def online_ingest(table: TableName, rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    if not (1 <= len(rows) <= 1000):
        raise HTTPException(400, detail="batch size must be 1..1000")

    if table == "departments":
        data = _validate_departments(rows)
        sql  = text("INSERT INTO app.departments(id,name) VALUES(:id,:name) ON CONFLICT(id) DO NOTHING")
    elif table == "jobs":
        data = _validate_jobs(rows)
        sql  = text("INSERT INTO app.jobs(id,name) VALUES(:id,:name) ON CONFLICT(id) DO NOTHING")
    elif table == "employees":
        data = _validate_employees(rows)
        sql  = text("""INSERT INTO app.employees(id,name,dt,department_id,job_id)
                       VALUES(:id,:name,:dt,:department_id,:job_id)
                       ON CONFLICT(id) DO NOTHING""")
    else:
        raise HTTPException(400, detail="unknown table")

    with engine.begin() as conn:
        res = conn.execute(sql, data)
        inserted = res.rowcount or 0

    return {"table": table, "received": len(rows), "inserted": inserted}
