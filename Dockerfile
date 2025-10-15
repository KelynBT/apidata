FROM python:3.11-slim

# Evita prompts
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# Dependencias del sistema mínimas
RUN apt-get update -y && apt-get install -y --no-install-recommends \
    build-essential curl && \
    rm -rf /var/lib/apt/lists/*

# App
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app

# Puerto de uvicorn
EXPOSE 8000

# Comando
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
