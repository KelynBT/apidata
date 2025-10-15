from .db import ping_db
from .s3 import ping_s3
from fastapi import FastAPI, Query, Body, HTTPException
from pydantic import BaseModel, Field
from typing import Any, Dict, List, Literal

from .ingest import ingest_departments, ingest_jobs, ingest_employees
from .online import online_ingest
from .metrics import hires_by_quarter, departments_above_avg
from .backup import backup_table_parquet, restore_table_parquet

app = FastAPI(title="Mi API REST")

@app.get("/health")
def health():
    return {"status": "ok"}

@app.get("/db/ping")
def db_ping():
    try:
        val = ping_db()
        return {"db": "ok", "select_1": int(val)}
    except Exception as e:
        return {"db": "error", "detail": str(e)}

@app.get("/s3/ping")
def s3_ping():
    return ping_s3()

# --- Ingesta ---
@app.post("/ingest/departments")
def api_ingest_departments():
    try:
        return ingest_departments()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/ingest/jobs")
def api_ingest_jobs():
    try:
        return ingest_jobs()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/ingest/employees")
def _ing_emp(
    limit: int | None = Query(None, ge=1),
    batch_size: int | None = Query(None, ge=1, le=50000),
    progress_every: int = Query(10000, ge=1),
):
    return ingest_employees(limit=limit, batch_size=batch_size, progress_every=progress_every)


# Online ingest
class OnlinePayload(BaseModel):
    table: Literal["departments","jobs","employees"]
    rows: List[Dict[str, Any]] = Field(min_items=1, max_items=1000)

@app.post("/online/ingest")
def _online_ingest(payload: OnlinePayload):
    return online_ingest(payload.table, payload.rows)

# Métricas
@app.get("/metrics/hires-by-quarter")
def _m1(year: int = Query(..., ge=1900, le=2100)):
    return {"year": year, "rows": hires_by_quarter(year)}

@app.get("/metrics/top-departments")
def _m2(year: int = Query(..., ge=1900, le=2100)):
    return {"year": year, "rows": departments_above_avg(year)}

# Backups
@app.post("/backup/{table}")
def _backup(table: Literal["departments","jobs","employees"]):
    return backup_table_parquet(table)

class RestorePayload(BaseModel):
    key: str
    batch_size: int | None = 1000

@app.post("/restore/{table}")
def _restore(table: Literal["departments","jobs","employees"], payload: RestorePayload = Body(...)):
    return restore_table_parquet(table, payload.key, payload.batch_size or 1000)