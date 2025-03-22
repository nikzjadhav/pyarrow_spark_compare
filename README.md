# pyarrow_spark_compare
comparing pyarrow and spark parquet read write 

# PyArrow vs PySpark Parquet Performance Comparison

## Overview
This project compares the performance of PyArrow and PySpark in reading and writing Parquet files. The script measures the execution time for both libraries when handling Parquet files.

## Requirements
Ensure you have the following dependencies installed:


## How It Works
1. **Initialize Spark Session** - Required for using PySpark.
2. **Create and Write Parquet**
   - PyArrow: Uses `pa.Table.from_pydict()` and `pq.write_table()`.
   - PySpark: Uses `spark.createDataFrame()` and `df.write.parquet()`.
3. **Read Parquet**
   - PyArrow: Uses `pq.read_table()`.
   - PySpark: Uses `spark.read.parquet()`.
4. **Performance Comparison**
   - Time is measured for both reading and writing operations.
   - Prints execution times for PyArrow and PySpark.

## Running the Script
Execute the script using:

```bash
python script.py
```

## Expected Output
The script will output the execution times for both PyArrow and PySpark:

```bash
PyArrow write time: 0.002345 seconds
PyArrow read time: 0.001789 seconds
Spark write time: 0.034567 seconds
Spark read time: 0.021456 seconds
```

## Conclusion
This comparison helps determine which library is better suited for specific use cases based on performance metrics. PyArrow is generally faster for smaller datasets, whereas PySpark scales better for larger datasets.

## License
This project is open-source and free to use.

