from pyspark.sql import SparkSession
import os
import sys 
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
# Create Spark session (local mode with 4 worker threads)
spark = SparkSession.builder \
    .appName("SparkWorkerTest") \
    .master("local[4]") \
    .getOrCreate()

# Print Spark configuration to verify
print("=== Spark Configuration ===")
for k, v in spark.sparkContext.getConf().getAll():
    print(f"{k}: {v}")

# Simple distributed computation
print("\n=== Worker Test ===")
rdd = spark.sparkContext.parallelize(range(1, 1000000), 8)
result = rdd.map(lambda x: x ** 2).sum()
print(f"Sum of squares (1..1,000,000): {result}")

# Check Spark web UI port
print("\nSpark UI running on:", spark.sparkContext.uiWebUrl)

# Clean up
spark.stop()
