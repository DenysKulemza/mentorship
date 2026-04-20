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


# 100k records

# Full dataset read
# Performance Comparison:
# CSV: 2.36s
# JSON: 0.48s
# Parquet: 0.35s


# Filter + Select
# Performance Comparison:
# CSV: 2.36s
# JSON: 0.53s
# Parquet: 0.66s

# 3.4M Apr  6 18:34 data.csv
# 8.3M Apr  6 18:34 data.json
# 1.4M Apr  6 18:34 data.parquet


# 1.5mil

# 54M Apr  6 18:40 data.csv
# 126M Apr  6 18:40 data.json
# 20M Apr  6 18:40 data.parquet

# Full dataset read
# Performance Comparison:
# CSV: 2.67s
# JSON: 1.79s
# Parquet: 0.33s

# Filter + Select
# Performance Comparison:
# CSV: 3.65s
# JSON: 2.22s
# Parquet: 0.84s