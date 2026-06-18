# Pure Apache Spark Image -
FROM apache/spark:3.5.1

USER root

ENV PYTHONUNBUFFERED=1
# עדכון ה-PATH כדי שפקודות ספארק יהיו זמינות גלובלית
ENV PATH="/opt/spark/bin:${PATH}"

# התקנת כלי רשת בסיסיים ופייתון
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl python3-pip \
    && rm -rf /var/lib/apt/lists/*

# הורדת הדרייברים (JARs) של קפקא ופוסטגרס ישירות לתיקיית ה-jars של ספארק
WORKDIR /opt/spark/jars
RUN curl -O https://jdbc.postgresql.org/download/postgresql-42.7.3.jar
RUN curl -O https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.1/spark-token-provider-kafka-0-10_2.12-3.5.1.jar \
    && curl -O https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.1/spark-sql-kafka-0-10_2.12-3.5.1.jar \
    && curl -O https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar \
    && curl -O https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar

# התקנת הדרייברים של פייתון וספריית pyspark התואמת
RUN pip3 install --no-cache-dir neo4j==5.19.0 psycopg2-binary==2.9.9 pyspark==3.5.1

WORKDIR /app