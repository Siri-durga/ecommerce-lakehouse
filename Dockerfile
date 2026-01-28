FROM apache/spark:3.5.0

USER root

RUN pip install --no-cache-dir boto3 delta-spark pyspark

WORKDIR /app

COPY app /app
COPY data /data

CMD ["spark-submit", "--master", "spark://spark-master:7077", "main.py"]
