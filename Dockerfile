FROM python:3.12-slim

WORKDIR /app

# Persistent data directory for SQLite DB
RUN mkdir -p /data

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Pre-initialise the database schema at build time (no-op if DB already exists)
RUN python -c "from core.db import init_db; init_db()" 2>/dev/null || true

EXPOSE 5000

VOLUME ["/data"]

CMD ["python", "main.py"]
