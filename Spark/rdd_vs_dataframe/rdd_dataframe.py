from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# RDD vs DataFrame Examples in PySpark


# Initialize Spark Session
spark = SparkSession.builder.appName("RDD_vs_DataFrame").getOrCreate()
sc = spark.sparkContext

# ============== RDD EXAMPLES ==============
# RDD (Resilient Distributed Dataset):
# - Low-level, unstructured data abstraction
# - No schema enforcement
# - Slower for SQL operations
# - More flexible for unstructured data

# Create an RDD from a collection
rdd_data = sc.parallelize([1, 2, 3, 4, 5])
result_rdd = rdd_data.map(lambda x: x * 2).collect()
print("RDD result:", result_rdd)

# RDD with tuples (key-value pairs)
rdd_kv = sc.parallelize([("Alice", 25), ("Bob", 30), ("Charlie", 35)])
rdd_filtered = rdd_kv.filter(lambda x: x[1] > 26).collect()
print("RDD filtered:", rdd_filtered)

# ============== DATAFRAME EXAMPLES ==============
# DataFrame:
# - High-level, structured data abstraction
# - Enforces schema (column names, types)
# - Optimized by Catalyst optimizer
# - Better performance for SQL operations
# - Closer to SQL/Pandas

# Create a DataFrame from data with schema
data = [("Alice", 25), ("Bob", 30), ("Charlie", 35)]
schema = StructType([
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True)
])
df = spark.createDataFrame(data, schema=schema)

# DataFrame operations using SQL-like syntax
df_filtered = df.filter(df.age > 26)
df_filtered.show()

# SQL query on DataFrame
df.createOrReplaceTempView("people")
result_sql = spark.sql("SELECT name, age FROM people WHERE age > 26")
result_sql.show()

# ============== KEY DIFFERENCES ==============
# 1. Schema: RDD has no schema | DataFrame has explicit schema
# 2. Performance: RDD is slower | DataFrame is optimized
# 3. SQL: RDD doesn't support SQL | DataFrame fully supports SQL
# 4. Memory: RDD uses more memory | DataFrame uses less (optimized storage)
# 5. Use case: RDD for unstructured data | DataFrame for structured data