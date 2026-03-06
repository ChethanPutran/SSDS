from pyspark.sql import SparkSession
import os
import socket

spark = SparkSession.builder.appName("CheckWorkers").getOrCreate()

def check_path(x):
    path = "/usr/local/hadoop/bin/hdfs"
    exists = os.path.exists(path)
    return f">>> NODE: {socket.gethostname()} | HDFS EXISTS: {exists}"

# Parallelize creates 2 tasks for your 2 executors
results = spark.sparkContext.parallelize(range(2), 2).map(check_path).collect()

for r in results:
    print(r)

spark.stop()
