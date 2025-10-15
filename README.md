# API REST — Ingesta, Métricas y Backups

FastAPI + PostgreSQL (RDS) + S3 + Docker

API productiva para cargar datos desde S3 a PostgreSQL en lotes, exponer métricas, y hacer backup/restore en Parquet sobre S3. Incluye ingesta “online” (por JSON), validaciones, auditoría y endpoints de salud.

# Arquitectura

FastAPI (Uvicorn) en Docker (app/main.py).

PostgreSQL (RDS): conexión con SQLAlchemy/psycopg (app/db.py).

AWS S3: lectura de CSV (prefijo raw/) y escritura de errores + Parquet (prefijo backup/) (app/s3.py, app/backup.py).

Ingesta batch desde S3 con validaciones y lotes (app/ingest.py).

Ingesta online (filas en el cuerpo del request) (app/online.py).

Métricas SQL puras (app/metrics.py).

# Requisitos

Docker + Docker Compose

RDS Postgres accesible desde la instancia

Bucket S3 con carpetas raw/ y backup/

Rol de instancia EC2 con permisos S3:

s3:ListBucket en el bucket

s3:GetObject en raw/*

s3:PutObject en backup/*

# Variables de entorno - Example File

### ===== Postgres (RDS) =====
DB_HOST=your-rds-endpoint.rds.amazonaws.com
DB_PORT=5432
DB_NAME=apirest_prod
DB_USER=app_user
DB_PASS=__SET_AT_RUNTIME__

### ===== AWS / S3 =====
AWS_REGION=us-east-1
S3_BUCKET=your-bucket
S3_PREFIX=raw/
BACKUP_PREFIX=backup/

### ===== Ingesta =====
BATCH_SIZE=1000
DEPARTMENTS_FILE=departments.csv
JOBS_FILE=jobs.csv
EMPLOYEES_FILE=hired_employees.csv

# Puesta en marcha
### 1) Build & run
docker compose up -d --build

### 2) Health checks
curl -s http://localhost:8000/health
curl -s http://localhost:8000/db/ping
curl -s http://localhost:8000/s3/ping

### 3) Ver endpoints
curl -s http://localhost:8000/openapi.json | jq '.paths | keys'

# Modelo de datos (esquema app)
CREATE SCHEMA IF NOT EXISTS app;

CREATE TABLE IF NOT EXISTS app.departments(
  id   INT PRIMARY KEY,
  name TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS app.jobs(
  id   INT PRIMARY KEY,
  name TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS app.employees(
  id            INT PRIMARY KEY,
  name          TEXT NOT NULL,
  dt            TIMESTAMP WITHOUT TIME ZONE NOT NULL,
  department_id INT NOT NULL REFERENCES app.departments(id),
  job_id        INT NOT NULL REFERENCES app.jobs(id)
);

CREATE TABLE IF NOT EXISTS app.ingest_audit(
  id           BIGSERIAL PRIMARY KEY,
  file_name    TEXT,
  table_target TEXT,
  total_read   INT,
  valid_rows   INT,
  invalid_rows INT,
  batch_size   INT,
  error_s3_key TEXT,
  created_at   TIMESTAMP DEFAULT NOW()
);


Las inserciones usan ON CONFLICT(id) DO NOTHING para evitar duplicados.

# Endpoints principales
Ingesta desde S3 (batch)
### departments.csv (s3://$S3_BUCKET/raw/)
curl -s -X POST http://localhost:8000/ingest/departments | jq

### jobs.csv
curl -s -X POST http://localhost:8000/ingest/jobs | jq

### hired_employees.csv (parámetros opcionales)
#   limit: cuántas filas leer (testing)
#   batch_size: tamaño de lote
#   progress_every: frecuencia de logs de progreso
curl -s -X POST \
  "http://localhost:8000/ingest/employees?limit=10000&batch_size=1000&progress_every=20000" | jq


Filas inválidas se guardan en s3://$S3_BUCKET/backup/errors/... (CSV o JSONL, según volumen).

Auditoría en tabla app.ingest_audit.

# Ingesta online (JSON)
### departments
curl -s -X POST http://localhost:8000/online/ingest \
  -H 'Content-Type: application/json' \
  -d '{"table":"departments","rows":[{"id":999,"name":"Dept API"}]}' | jq

### employees (usar IDs reales de dept/job)
curl -s -X POST http://localhost:8000/online/ingest \
  -H 'Content-Type: application/json' \
  -d '{
        "table":"employees",
        "rows":[
          {"id":2000002,"name":"Ana API","datetime":"2024-02-01 08:00:00","department_id":10,"job_id":292}
        ]
      }' | jq

# Métricas
### Contrataciones por trimestre (por depto / job)
curl -s "http://localhost:8000/metrics/hires-by-quarter?year=2021" | jq

### Departamentos por encima del promedio (año)
curl -s "http://localhost:8000/metrics/top-departments?year=2021" | jq

Backup & Restore (Parquet en S3)
### Backup tabla -> s3://$S3_BUCKET/backup/parquet/employees/<file>.parquet
curl -s -X POST http://localhost:8000/backup/employees \
  | tee /tmp/last-backup.json | jq

### Usar la clave devuelta para restaurar (no duplica IDs existentes)
S3KEY=$(jq -r '.s3_key' /tmp/last-backup.json)
curl -s -X POST http://localhost:8000/restore/employees \
  -H 'Content-Type: application/json' \
  -d "{\"key\":\"$S3KEY\",\"batch_size\":1000}" | jq

# Troubleshooting & Operación

Logs en vivo
docker compose logs -f api

tmux (recomendado para jobs largos)

Crear: tmux new -s ingest

Detach: Ctrl+b, d

Re-attach: tmux attach -t ingest

Cambiar tamaño de lote
Usar ?batch_size=... en el endpoint o BATCH_SIZE en .env.

Validación de S3/DB
curl -s http://localhost:8000/s3/ping / curl -s http://localhost:8000/db/ping

# Estructura
.
├─ app/
│  ├─ main.py         # rutas FastAPI
│  ├─ db.py           # engine y ping DB
│  ├─ s3.py           # cliente S3 y ping
│  ├─ ingest.py       # ingestas batch (S3 → Postgres)
│  ├─ online.py       # ingesta online (JSON)
│  ├─ metrics.py      # endpoints de métricas
│  └─ backup.py       # backup/restore Parquet en S3
├─ Dockerfile
├─ docker-compose.yml
├─ requirements.txt
├─ .env.example       # plantilla (sin secretos)
└─ .gitignore


.parquet
