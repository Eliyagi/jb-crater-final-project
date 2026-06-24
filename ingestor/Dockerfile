# Production-ready Python Ingestor for Crater
FROM python:3.11-slim

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*

# Pinned dependencies including PostgreSQL and Neo4j drivers
RUN pip install --no-cache-dir \
    requests==2.31.0 \
    kafka-python-ng==2.2.2 \
    psycopg2-binary==2.9.9 \
    neo4j==5.21.0

# Copy all infrastructure scripts inside
COPY ingestor/main.py /app/main.py
#COPY src/serving.py /app/serving.py

# Changed to bash so it doesn't auto-run main.py anymore, 
# allowing you to execute any script from the outside.
CMD ["/bin/bash"]