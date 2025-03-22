import pyarrow as pa
import pyarrow.parquet as pq
import pyspark
from pyspark.sql import SparkSession
import time

# Initialize Spark Session
spark = SparkSession.builder.appName("ParquetPerformanceComparison").getOrCreate()

# Function to create and write a PyArrow table
def create_and_write_parquet_pyarrow(filename):
    data = {
        "id": [1, 2, 3],
        "name": ["Alice", "Bob", "Charlie"],
        "age": [25, 30, 35]
    }
    table = pa.Table.from_pydict(data)
    pq.write_table(table, filename)
    return filename

# Function to read a Parquet file with PyArrow
def read_parquet_pyarrow(filename):
    return pq.read_table(filename)

# Function to create and write a Spark DataFrame as Parquet
def create_and_write_parquet_spark(filename):
    data = [(1, "Alice", 25), (2, "Bob", 30), (3, "Charlie", 35)]
    df = spark.createDataFrame(data, ["id", "name", "age"])
    df.write.mode("overwrite").parquet(filename)
    return filename

# Function to read a Parquet file with Spark
def read_parquet_spark(filename):
    return spark.read.parquet(filename)

# Compare PyArrow and Spark for Parquet operations
def compare_parquet_performance():
    filename = "sample.parquet"
    
    # PyArrow Write
    start_time = time.time()
    create_and_write_parquet_pyarrow(filename)
    pyarrow_write_time = time.time() - start_time
    
    # PyArrow Read
    start_time = time.time()
    table = read_parquet_pyarrow(filename)
    pyarrow_read_time = time.time() - start_time
    
    # Spark Write
    start_time = time.time()
    create_and_write_parquet_spark(filename)
    spark_write_time = time.time() - start_time
    
    # Spark Read
    start_time = time.time()
    df = read_parquet_spark(filename)
    spark_read_time = time.time() - start_time
    
    print(f"PyArrow write time: {pyarrow_write_time:.6f} seconds")
    print(f"PyArrow read time: {pyarrow_read_time:.6f} seconds")
    print(f"Spark write time: {spark_write_time:.6f} seconds")
    print(f"Spark read time: {spark_read_time:.6f} seconds")

if __name__ == "__main__":
    compare_parquet_performance()
    spark.stop()

