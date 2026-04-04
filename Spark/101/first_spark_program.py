from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder.appName("FirstSparkProgram").getOrCreate()

# Create a simple DataFrame
data = [("Alice", 25), ("Bob", 30), ("Charlie", 35)]
columns = ["Name", "Age"]

df = spark.createDataFrame(data, columns)

# ........

# df_result =...

# Dataframe = immutable

# Display the DataFrame
# df.show()

# Basic operations
# df.printSchema()
# df_selected = df.select("Name")
# df.select("Name").show()
# df_filtered = df.filter(df.Age > 28)
# df.filter(df.Age > 28).show()

# df.show()

# # # Narrow Transformation Example (data stays within partitions)
# # print("=== NARROW TRANSFORMATION ===")
# df_narrow = df.filter(df.Age > 28)  # No shuffle required
# df_narrow.show()

# # # Wide Transformation Example (requires shuffle/redistribution)
# print("=== WIDE TRANSFORMATION ===")
# df_wide = df.groupBy("Age").count()  # Requires shuffle
# df_wide.show()

# # # Another narrow transformation
# print("=== NARROW: Select ===")
# df_select = df.select("Name", "Age")
# df_select.show()

df_filter = df.filter(df.Age > 28)
df_select = df_filter.select("Name")

df_select.show()

# # Another wide transformation
# print("=== WIDE: Join ===")
# df2 = spark.createDataFrame([("Alice", "Engineer"), ("Bob", "Manager")], ["Name", "Job"])
# df_result = df_filtered.join(df2, "Name")  # Requires shuffle
# df_join.show()

# # Caching example
# print("=== CACHING ===")
# df_filtered.cache()  # Cache the filtered DataFrame
# df_filtered.show()  # This will use the cached data

# # Unpersist the cached DataFrame
# df_filtered.unpersist()
# # Re-cache the filtered DataFrame
# df_filtered.cache()

# Perform some transformations and actions on the cached DataFrame
# print("=== CACHED DATAFRAME ACTIONS ===")
# df_result.cache() 
# df_result.show()  # This will use the cached data

# # Perform a count operation
# count = df_result.count()
# print(f"Count of filtered DataFrame: {count}")

# # Perform a groupBy operation
# grouped_df = df_result.groupBy("Age").count()
# grouped_df.show()

# # Unpersist the cached DataFrame after use
# df_filtered.unpersist()

# Stop the Spark session
spark.stop()