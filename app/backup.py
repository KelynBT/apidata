import io, os, uuid, datetime, pandas as pd
from sqlalchemy import text
import boto3
from .db import engine

BUCKET        = os.getenv("S3_BUCKET")
REGION        = os.getenv("AWS_REGION", "us-east-1")
BACKUP_PREFIX = os.getenv("BACKUP_PREFIX", "backup/")
s3 = boto3.client("s3", region_name=REGION)

def backup_table_parquet(table: str) -> dict:
    with engine.begin() as conn:
        df = pd.read_sql(text(f"SELECT * FROM app.{table}"), conn)
    ts  = datetime.datetime.utcnow().strftime("%Y%m%dT%H%M%S")
    key = f"{BACKUP_PREFIX.rstrip('/')}/parquet/{table}/{table}-{ts}-{uuid.uuid4().hex}.parquet"
    bio = io.BytesIO()
    df.to_parquet(bio, index=False)
    s3.put_object(Bucket=BUCKET, Key=key, Body=bio.getvalue())
    return {"table": table, "rows": len(df), "s3_key": key}

def restore_table_parquet(table: str, key: str, batch_size: int = 1000) -> dict:
    obj = s3.get_object(Bucket=BUCKET, Key=key)
    df  = pd.read_parquet(io.BytesIO(obj["Body"].read()))
    rows = df.to_dict(orient="records")

    if table == "departments":
        sql = text("INSERT INTO app.departments(id,name) VALUES(:id,:name) ON CONFLICT(id) DO NOTHING")
    elif table == "jobs":
        sql = text("INSERT INTO app.jobs(id,name) VALUES(:id,:name) ON CONFLICT(id) DO NOTHING")
    elif table == "employees":
        sql = text("""INSERT INTO app.employees(id,name,dt,department_id,job_id)
                      VALUES(:id,:name,:dt,:department_id,:job_id) ON CONFLICT(id) DO NOTHING""")
    else:
        raise ValueError("table not allowed")

    inserted = 0
    with engine.begin() as conn:
        for i in range(0, len(rows), batch_size):
            batch = rows[i:i+batch_size]
            res = conn.execute(sql, batch)
            inserted += res.rowcount or 0
    return {"table": table, "restored": inserted, "from_s3": key}
