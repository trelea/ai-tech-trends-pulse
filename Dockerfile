FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Minimal runtime for main.py: collector + R2/Turso upsert helper.
# smm_agent.py / models.py / migrate.py intentionally excluded.
COPY main.py db.py ./

ENTRYPOINT ["python", "main.py"]
