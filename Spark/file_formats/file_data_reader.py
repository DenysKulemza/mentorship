from pyspark.sql import SparkSession
import time

# Create Spark session
spark = SparkSession.builder.appName("FileFormatPerformanceComparison").getOrCreate()

# Define file paths (replace with actual paths to your CSV, JSON, and Parquet files)
csv_path = "/Users/dkulemza/PycharmProjects/mentorship/Spark/file_formats/data.csv"
json_path = "/Users/dkulemza/PycharmProjects/mentorship/Spark/file_formats/data.json"
parquet_path = "/Users/dkulemza/PycharmProjects/mentorship/Spark/file_formats/data.parquet"

# Function to measure read time
def measure_read_time(format_name, path, format_type):
    start_time = time.time()
    if format_type == "csv":
        df = spark.read.csv(path, header=True, inferSchema=True)
        df = df.select('city', 'first_name').filter(df.city == 'Jonesview')
    elif format_type == "json":
        df = spark.read.json(path)
        df = df.select('city', 'first_name').filter(df.city == 'Jonesview')
    elif format_type == "parquet":
        df = spark.read.parquet(path)
        df = df.select('city', 'first_name').filter(df.city == 'Jonesview')
    df.count()  # Trigger action to read data
    end_time = time.time()
    read_time = end_time - start_time
    print(f"{format_name} read time: {read_time:.2f} seconds")
    return read_time

# Measure and compare performance
csv_time = measure_read_time("CSV", csv_path, "csv")
json_time = measure_read_time("JSON", json_path, "json")
parquet_time = measure_read_time("Parquet", parquet_path, "parquet")

# Print comparison
print("\nPerformance Comparison:")
print(f"CSV: {csv_time:.2f}s")
print(f"JSON: {json_time:.2f}s")
print(f"Parquet: {parquet_time:.2f}s")

# Stop Spark session
spark.stop()