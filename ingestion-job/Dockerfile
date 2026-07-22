# No JRE here — unlike training-job/streaming-job, ingestion-job never
# touches Spark. It's a plain Python process reading HDFS over WebHDFS
# and producing to Kafka, so the base slim image needs nothing extra.
FROM python:3.10-slim

WORKDIR /app

ENV PYTHONUNBUFFERED=1

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY producer.py .

CMD ["python", "producer.py"]