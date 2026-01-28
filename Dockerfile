FROM apache/spark:3.5.0

USER root

RUN pip install --no-cache-dir boto3 delta-spark

WORKDIR /app

COPY app /app
COPY data /data

CMD ["spark-submit","--master","spark://spark-master:7077","--packages","io.delta:delta-spark_2.12:3.1.0,org.apache.hadoop:hadoop-aws:3.3.4","main.py"]
