FROM apache/spark:3.5.1

USER root

# יצירת תיקיית ה-jars הפנימית של ספארק
RUN mkdir -p /opt/spark/jars

# הורדת כל ה-JARs הנדרשים ישירות לתוך ה-Image בזמן הבנייה (Build Time)
RUN curl -fsSL -o /opt/spark/jars/postgresql-42.7.3.jar https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar && \
    curl -fsSL -o /opt/spark/jars/neo4j-connector-apache-spark_2.12-5.3.1_for_spark_3.jar https://repo1.maven.org/maven2/org/neo4j/neo4j-connector-apache-spark_2.12/5.3.1_for_spark_3/neo4j-connector-apache-spark_2.12-5.3.1_for_spark_3.jar && \
    curl -fsSL -o /opt/spark/jars/spark-sql-kafka-0-10_2.12-3.5.1.jar https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.1/spark-sql-kafka-0-10_2.12-3.5.1.jar && \
    curl -fsSL -o /opt/spark/jars/kafka-clients-3.4.1.jar https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar && \
    curl -fsSL -o /opt/spark/jars/spark-token-provider-kafka-0-10_2.12-3.5.1.jar https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.1/spark-token-provider-kafka-0-10_2.12-3.5.1.jar && \
    curl -fsSL -o /opt/spark/jars/commons-pool2-2.11.1.jar https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar

WORKDIR /app