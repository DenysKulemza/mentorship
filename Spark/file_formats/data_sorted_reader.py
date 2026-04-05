from pyspark.sql import SparkSession
import time

# Initialize Spark session
spark = SparkSession.builder.appName("ParquetReadComparison").getOrCreate()

# Path to Parquet file (replace with actual path)
sorted_parquet_path = "/Users/dkulemza/PycharmProjects/mentorship/Spark/file_formats/sorted_output.parquet"
unsorted_parquet_path = "/Users/dkulemza/PycharmProjects/mentorship/Spark/file_formats/unsorted_output.parquet"

# Read unsorted data and measure time
start_time = time.time()
unsorted_df = spark.read.parquet(sorted_parquet_path)
unsorted_df = unsorted_df.filter(unsorted_df.age > 22)
unsorted_count = unsorted_df.count()
unsorted_time = time.time() - start_time

# Read data and sort it, then measure time
start_time = time.time()
sorted_df = spark.read.parquet(unsorted_parquet_path)
sorted_df = sorted_df.filter(sorted_df.age > 22)
sorted_count = sorted_df.count()
sorted_time = time.time() - start_time

# Print results
print(f"Unsorted read time: {unsorted_time:.2f} seconds, Count: {unsorted_count}")
print(f"Sorted read time: {sorted_time:.2f} seconds, Count: {sorted_count}")

# Stop Spark session
spark.stop()