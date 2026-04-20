from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Create SparkSession
spark = SparkSession.builder.appName("DataSorting").getOrCreate()

# Read parquet file (replace 'input.parquet' with your file path)
df = spark.read.parquet("/Users/dkulemza/PycharmProjects/mentorship/Spark/file_formats/data.parquet")

# Print number of partitions for unsorted dataframe
print(f"Unsorted dataframe partitions: {df.rdd.getNumPartitions()}")

# Unsorted dataframe partitions: 6

# Write unsorted data
df.write.parquet("unsorted_output.parquet")

# Sort data by a column (replace 'column_name' with actual column)
sorted_df = df.orderBy(col("age"))

# Sorted dataframe partitions: 9

# Print number of partitions for sorted dataframe
print(f"Sorted dataframe partitions: {sorted_df.rdd.getNumPartitions()}")

# Write sorted data
sorted_df.write.parquet("sorted_output.parquet")

# Stop SparkSession
spark.stop()
