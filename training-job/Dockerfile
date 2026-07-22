# Slim base — no build tools, no JDK (JRE headless is enough to run)
# PySpark's JVM side; we're not compiling anything in this image).
FROM python:3.10-slim

# default-jre-headless is compatible with Spark 3.5.x. --no-install-recommends
RUN apt-get update && \
    apt-get install -y --no-install-recommends default-jre-headless procps && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Unbuffered stdout so print() output isn't delayed relative to Spark's log4j output.
ENV PYTHONUNBUFFERED=1

# Requirements copied and installed before the code so Docker caches this
# layer — editing train.py won't force a full pip reinstall on rebuild.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY train.py .

CMD ["python", "train.py"]