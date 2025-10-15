# -*- coding: utf-8 -*-
import os, csv, io, boto3, datetime, uuid, time, json, logging
from typing import Dict, Any, List, Iterable, Optional
from sqlalchemy import text
from .db import engine

# --- env ---
BUCKET        = os.getenv("S3_BUCKET")
RAW_PREFIX    = os.getenv("S3_PREFIX", "raw/")
BACKUP_PREFIX = os.getenv("BACKUP_PREFIX", "backup/")
REGION        = os.getenv("AWS_REGION", "us-east-1")
BATCH_SIZE    = int(os.getenv("BATCH_SIZE", "1000"))

DEPARTMENTS_FILE = os.getenv("DEPARTMENTS_FILE", "departments.csv")
JOBS_FILE        = os.getenv("JOBS_FILE", "jobs.csv")
EMPLOYEES_FILE   = os.getenv("EMPLOYEES_FILE", "hired_employees.csv")

s3  = boto3.client("s3", region_name=REGION)
log = logging.getLogger("uvicorn")

# ---------- utils ----------
def _normalize(name: str) -> str:
    return name.replace("\ufeff","").strip().lower().replace(" ", "_")

def _read_csv_from_s3(key: str, expected_headers: Optional[List[str]] = None) -> List[Dict[str, Any]]:
    obj = s3.get_object(Bucket=BUCKET, Key=key)
    raw = obj["Body"].read().decode("utf-8-sig", errors="replace")
    reader = csv.reader(io.StringIO(raw), delimiter=",")
    rows = list(reader)
    if not rows:
        return []
    headers = [_normalize(h) for h in rows[0]]
    if expected_headers and (("id" not in headers) or any(h not in headers for h in expected_headers if h)):
        headers = expected_headers[:]
        data_rows = rows
    else:
        data_rows = rows[1:]
    out: List[Dict[str, Any]] = []
    for r in data_rows:
        out.append({headers[i]: (r[i] if i < len(headers) and i < len(r) else "") for i in range(len(headers))})
    return out

def _iter_employees_from_s3(key: str):
    obj = s3.get_object(Bucket=BUCKET, Key=key)
    tw = io.TextIOWrapper(obj["Body"], encoding="utf-8-sig", newline="")
    reader = csv.DictReader(tw)
    for row in reader:
        yield { _normalize(k): (v or "").strip() for k, v in row.items() }

def _parse_dt(s: str):
    if not s or s.strip()=="":
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d",
                "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S.%fZ"):
        try:
            return datetime.datetime.strptime(s.strip(), fmt)
        except ValueError:
            continue
    try:
        return datetime.datetime.fromisoformat(s.strip().replace("Z",""))
    except Exception:
        return None

def _chunks(lst: List[Dict[str, Any]], size: int) -> Iterable[List[Dict[str, Any]]]:
    for i in range(0, len(lst), size):
        yield lst[i:i+size]

def _write_errors_csv_to_s3(file_tag: str, rows: List[Dict[str, Any]]) -> str:
    if not rows:
        return ""
    key = f"{BACKUP_PREFIX.rstrip('/')}/errors/{file_tag}-{uuid.uuid4().hex}.csv"
    buf = io.StringIO()
    headers = sorted({k for r in rows for k in r.keys() if k != "_reason"})
    writer = csv.writer(buf)
    writer.writerow(headers + ["reason"])
    for r in rows:
        writer.writerow([r.get(h, "") for h in headers] + [r.get("_reason","")])
    s3.put_object(Bucket=BUCKET, Key=key, Body=buf.getvalue().encode("utf-8"))
    return key

def _write_errors_jsonl_to_s3(file_tag: str, rows: List[Dict[str, Any]]) -> str:
    if not rows:
        return ""
    ts  = datetime.datetime.utcnow().strftime("%Y%m%dT%H%M%S")
    key = f"{BACKUP_PREFIX.rstrip('/')}/errors/{file_tag}-{ts}.jsonl"
    b   = io.BytesIO()
    for r in rows:
        b.write((json.dumps(r, ensure_ascii=False) + "\n").encode("utf-8"))
    s3.put_object(Bucket=BUCKET, Key=key, Body=b.getvalue())
    return key

def _audit(file_name: str, table: str, total: int, valid: int, invalid: int, err_key: str, batch_size: int):
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO app.ingest_audit (file_name, table_target, total_read, valid_rows, invalid_rows, batch_size, error_s3_key)
            VALUES (:f, :t, :tr, :vr, :ir, :bs, :ek)
        """), {"f": file_name, "t": table, "tr": total, "vr": valid, "ir": invalid, "bs": batch_size, "ek": err_key})

# ---------- departments ----------
def ingest_departments() -> Dict[str, Any]:
    key  = f"{RAW_PREFIX.rstrip('/')}/" + DEPARTMENTS_FILE
    rows = _read_csv_from_s3(key, expected_headers=["id","name"])
    good, bad = [], []
    for r in rows:
        if not r.get("id"):
            r["_reason"]="missing id"; bad.append(r); continue
        if not r.get("name"):
            r["_reason"]="missing name"; bad.append(r); continue
        try:
            rid = int(r["id"])
        except:
            r["_reason"]="id not integer"; bad.append(r); continue
        good.append({"id": rid, "name": r["name"].strip()})
    sql = text("""INSERT INTO app.departments (id, name)
                  VALUES (:id, :name)
                  ON CONFLICT (id) DO NOTHING""")
    inserted = 0
    with engine.begin() as conn:
        for batch in _chunks(good, BATCH_SIZE):
            conn.execute(sql, batch); inserted += len(batch)
    err_key = _write_errors_csv_to_s3("departments_errors", bad)
    _audit(DEPARTMENTS_FILE, "app.departments", len(rows), len(good), len(bad), err_key, BATCH_SIZE)
    return {"file": key, "read": len(rows), "valid": len(good), "invalid": len(bad),
            "batch_size": BATCH_SIZE, "attempted": inserted, "errors_s3": err_key or None}

# ---------- jobs ----------
def ingest_jobs() -> Dict[str, Any]:
    key  = f"{RAW_PREFIX.rstrip('/')}/" + JOBS_FILE
    rows = _read_csv_from_s3(key, expected_headers=["id","name"])
    good, bad = [], []
    for r in rows:
        if not r.get("id"):
            r["_reason"]="missing id"; bad.append(r); continue
        if not r.get("name"):
            r["_reason"]="missing name"; bad.append(r); continue
        try:
            rid = int(r["id"])
        except:
            r["_reason"]="id not integer"; bad.append(r); continue
        good.append({"id": rid, "name": r["name"].strip()})
    sql = text("""INSERT INTO app.jobs (id, name)
                  VALUES (:id, :name)
                  ON CONFLICT (id) DO NOTHING""")
    inserted = 0
    with engine.begin() as conn:
        for batch in _chunks(good, BATCH_SIZE):
            conn.execute(sql, batch); inserted += len(batch)
    err_key = _write_errors_csv_to_s3("jobs_errors", bad)
    _audit(JOBS_FILE, "app.jobs", len(rows), len(good), len(bad), err_key, BATCH_SIZE)
    return {"file": key, "read": len(rows), "valid": len(good), "invalid": len(bad),
            "batch_size": BATCH_SIZE, "attempted": inserted, "errors_s3": err_key or None}

# ---------- employees ----------
def ingest_employees(limit: Optional[int] = None,
                     batch_size: Optional[int] = None,
                     progress_every: int = 10000) -> Dict[str, Any]:
    key = f"{RAW_PREFIX.rstrip('/')}/" + EMPLOYEES_FILE
    bs  = int(batch_size or BATCH_SIZE)

    with engine.begin() as conn:
        dept_ids = {row[0] for row in conn.execute(text("SELECT id FROM app.departments"))}
        job_ids  = {row[0] for row in conn.execute(text("SELECT id FROM app.jobs"))}

    insert_sql = text("""
        INSERT INTO app.employees (id, name, dt, department_id, job_id)
        VALUES (:id, :name, :dt, :department_id, :job_id)
        ON CONFLICT (id) DO NOTHING
    """)

    read = valid = invalid = inserted = 0
    bad: List[Dict[str, Any]] = []
    batch: List[Dict[str, Any]] = []
    t0 = time.time()

    for r in _iter_employees_from_s3(key):
        read += 1

        dep_val = r.get("department_id") or r.get("department") or r.get("departmer")
        dt_raw  = r.get("datetime") or r.get("dt")
        missing = [c for c in ("id","name","job_id") if not r.get(c)]
        if missing or not dep_val or not dt_raw:
            missing2 = []
            if not dep_val: missing2.append("department")
            if not dt_raw:  missing2.append("datetime")
            r["_reason"] = "missing fields: " + ",".join(missing + missing2)
            bad.append(r); invalid += 1
        else:
            try:
                rid  = int(r["id"]); rjob = int(r["job_id"]); rdep = int(dep_val)
            except Exception:
                r["_reason"]="id/department/job_id not integer"; bad.append(r); invalid += 1
            else:
                dt_val = _parse_dt(dt_raw)
                if dt_val is None:
                    r["_reason"]="invalid datetime"; bad.append(r); invalid += 1
                elif rdep not in dept_ids:
                    r["_reason"]=f"department {rdep} not in catalog"; bad.append(r); invalid += 1
                elif rjob not in job_ids:
                    r["_reason"]=f"job_id {rjob} not in catalog"; bad.append(r); invalid += 1
                else:
                    valid += 1
                    batch.append({
                        "id": rid,
                        "name": r["name"].strip(),
                        "dt": dt_val,          # usa .date() si la columna es DATE
                        "department_id": rdep,
                        "job_id": rjob,
                    })

        if batch and len(batch) >= bs:
            with engine.begin() as conn:
                res = conn.execute(insert_sql, batch)
                inserted += res.rowcount if res.rowcount is not None else 0
            batch.clear()

        if read % progress_every == 0:
            elapsed = time.time() - t0
            msg = f"[employees] read={read} valid={valid} invalid={invalid} inserted={inserted} elapsed={elapsed:0.1f}s"
            try: log.info(msg)
            except Exception: print(msg)

        if limit is not None and read >= limit:
            break

    if batch:
        with engine.begin() as conn:
            res = conn.execute(insert_sql, batch)
            inserted += res.rowcount if res.rowcount is not None else 0
        batch.clear()

    err_key = ""
    if bad:
        err_key = _write_errors_jsonl_to_s3("hired_employees_errors", bad) if len(bad) > 10000 \
                  else _write_errors_csv_to_s3("hired_employees_errors", bad)

    _audit(EMPLOYEES_FILE, "app.employees", read, valid, invalid, err_key, bs)

    return {
        "file": key, "read": read, "valid": valid, "invalid": invalid,
        "inserted": inserted, "batch_size": bs, "progress_every": progress_every,
        "limit": limit, "errors_s3": err_key or None, "elapsed_sec": round(time.time() - t0, 2),
    }