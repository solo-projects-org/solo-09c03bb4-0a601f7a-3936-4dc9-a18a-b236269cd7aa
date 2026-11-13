# postgres:16 already ships pg_restore/pg_dump 16 matching the db service.
FROM postgres:16

RUN apt-get update \
 && apt-get install -y --no-install-recommends python3 python3-venv \
 && rm -rf /var/lib/apt/lists/*

RUN python3 -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

WORKDIR /srv/app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 8000
# Single process on purpose: the in-process JobManager enforces the slot limit.
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
