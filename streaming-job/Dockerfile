# Same pattern as training-job: slim base + headless JRE only (PySpark's
# JVM side needs it; we're not compiling anything).
FROM python:3.10-slim

RUN apt-get update && \
    apt-get install -y --no-install-recommends default-jre-headless procps && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Unbuffered stdout — without this, print() output can lag far behind
# Spark's log4j output (which flushes immediately), especially in a
# long-running process like this one that never exits to force a flush.
ENV PYTHONUNBUFFERED=1

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY consumer.py .

CMD ["python", "consumer.py"]