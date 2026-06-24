# Spark driver image: Jupyter PySpark Notebook + Postgres & Neo4j & Kafka JARs.
# Version matrix: Spark 3.5.1 / Scala 2.12 / Python 3.11 

FROM quay.io/jupyter/pyspark-notebook:spark-3.5.1

USER root

# התקנת ספריות פייתון נדרשות עבור פרויקט Crater (כולל הדרייברים של פוסטגרס ו-Neo4j)
RUN pip install --no-cache-dir \
        pyspark==3.5.1 \
        duckdb==1.1.* \
        psycopg2-binary==2.9.9 \
        neo4j==5.19.0 \
        redis==5.* \
        jupytext==1.16.* \
        pytest==8.* \
        pytest-timeout==2.*

# הגדרת נתיב ה-Jars הרשמי של ספארק בתוך האימג' של Jupyter
RUN set -eux; \
    JARS=/usr/local/spark/jars; \
    # 1. הורדת הדרייבר של פוסטגרס (הגרסה התואמת)
    curl -fsSL -o "$JARS/postgresql-42.7.3.jar" https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar && \
    # 2. הורדת הדרייברים של קפקא והספריות הנלוות לטובת ה-Streaming
    curl -fsSL -o "$JARS/spark-token-provider-kafka-0-10_2.12-3.5.1.jar" https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.1/spark-token-provider-kafka-0-10_2.12-3.5.1.jar && \
    curl -fsSL -o "$JARS/spark-sql-kafka-0-10_2.12-3.5.1.jar" https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.1/spark-sql-kafka-0-10_2.12-3.5.1.jar && \
    curl -fsSL -o "$JARS/commons-pool2-2.11.1.jar" https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar && \
    curl -fsSL -o "$JARS/kafka-clients-3.4.1.jar" https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar && \
    # 3. תיקון הורדת הדרייברים הרשמיים של Neo4j לספארק (נתיב Maven מתוקן)
    curl -fsSL -o "$JARS/neo4j-connector-apache-spark_2.12-5.3.1_for_spark_3.jar" https://repo1.maven.org/maven2/org/neo4j/neo4j-connector-apache-spark_2.12/5.3.1_for_spark_3/neo4j-connector-apache-spark_2.12-5.3.1_for_spark_3.jar && \
    curl -fsSL -o "$JARS/neo4j-java-driver-5.19.0.jar" https://repo1.maven.org/maven2/org/neo4j/driver/neo4j-java-driver/5.19.0/neo4j-java-driver-5.19.0.jar && \
    # עדכון הרשאות לקבצים שהורדו
    chown -R ${NB_UID}:${NB_GID} "$JARS"

# העתקת קונפיגורציית ספארק במידה וקיימת אצלכם (אם אין, ספארק ישתמש ב-Defaults שלו)
COPY spark-defaults.conf /usr/local/spark/conf/spark-defaults.conf

USER ${NB_UID}

EXPOSE 8888 4040