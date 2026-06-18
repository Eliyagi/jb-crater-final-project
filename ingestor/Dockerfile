# Production-ready Python Ingestor for Crater
FROM python:3.11-slim

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*

# Pinned dependencies for stability
RUN pip install --no-cache-dir requests==2.31.0 kafka-python-ng==2.2.2

COPY main.py /app/main.py

CMD ["python", "main.py"]