import os
from sqlalchemy import text, create_engine
from sqlalchemy.engine import URL

DB_HOST = os.getenv("DB_HOST")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")

database_url = URL.create(
    "postgresql+psycopg",
    username=DB_USER,
    password=DB_PASS, 
    host=DB_HOST,
    port=DB_PORT,
    database=DB_NAME,
)

engine = create_engine(database_url, pool_pre_ping=True, pool_size=5, max_overflow=5)

def ping_db():
    with engine.connect() as conn:
        return conn.execute(text("SELECT 1")).scalar()


