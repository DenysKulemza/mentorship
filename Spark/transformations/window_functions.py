from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import sum as spark_sum, avg, rank, row_number

# Initialize SparkSession
spark = SparkSession.builder.appName("WindowFunctionsDemo").getOrCreate()

# Sample data
data = [
    ("Sales", "Alice", 1000),
    ("Sales", "Bob", 1500),
    ("IT", "Charlie", 2000),
    ("IT", "David", 1800),
    ("HR", "Eve", 1200),
]

df = spark.createDataFrame(data, ["Department", "Name", "Salary"])

print("Original DataFrame:")
df.show()

# ============ AGGREGATION (Reduces rows) ============
print("\n--- AGGREGATION (Groups data, reduces rows) ---")
agg_result = df.groupBy("Department").agg(
    spark_sum("Salary").alias("TotalSalary"),
    avg("Salary").alias("AvgSalary")
)
print("Aggregated by Department:")
agg_result.show()

# ============ WINDOW FUNCTIONS (Keeps all rows) ============
print("\n--- WINDOW FUNCTIONS (Keeps all rows) ---")

# Define window partitioned by Department
window_spec = Window.partitionBy("Department").orderBy("Salary")

# partitionBy == groupBy

window_result = df.withColumn(
    "DeptTotal", spark_sum("Salary").over(window_spec)
).withColumn(
    "DeptAvg", avg("Salary").over(window_spec)
).withColumn(
    "Rank", rank().over(window_spec)
).withColumn(
    "RowNum", row_number().over(window_spec)
)

# With Window Functions:
# +----------+-------+------+---------+-------+----+------+
# |Department|   Name|Salary|DeptTotal|DeptAvg|Rank|RowNum|
# +----------+-------+------+---------+-------+----+------+
# |        HR|    Eve|  1200|     1200| 1200.0|   1|     1|
# |        IT|  David|  1800|     1800| 1800.0|   1|     1|
# |        IT|Charlie|  2000|     3800| 1900.0|   2|     2|
# |     Sales|  Alice|  1000|     1000| 1000.0|   1|     1|
# |     Sales|    Bob|  1500|     2500| 1250.0|   2|     2|
# +----------+-------+------+---------+-------+----+------+

# First Window
# +----------+-------+------+---------+-------+----+------+
# |Department|   Name|Salary|DeptTotal|DeptAvg|Rank|RowNum|
# +----------+-------+------+---------+-------+----+------+
# |        HR|    Eve|  1200|     1200| 1200.0|   1|     1|

# Second Window
# +----------+-------+------+---------+-------+----+------+
# |Department|   Name|Salary|DeptTotal|DeptAvg|Rank|RowNum|
# +----------+-------+------+---------+-------+----+------+
# |        IT|  David|  1800|     1800| 1800.0|   1|     1|
# |        IT|Charlie|  2000|     3800| 1900.0|   2|     2|

# Third Window
# +----------+-------+------+---------+-------+----+------+
# |Department|   Name|Salary|DeptTotal|DeptAvg|Rank|RowNum|
# +----------+-------+------+---------+-------+----+------+
# |     Sales|  Alice|  1000|     1000| 1000.0|   1|     1|
# |     Sales|    Bob|  1500|     2500| 1250.0|   2|     2|

print("With Window Functions:")
window_result.show()

print("\nKey Difference:")
print(f"- Aggregation result rows: {agg_result.count()}")
print(f"- Window function result rows: {window_result.count()}")



# ============ DUPLICATES REMOVAL USING WINDOW FUNCTIONS ============
print("\n--- DUPLICATES REMOVAL USING WINDOW FUNCTIONS ---")

# Sample data with duplicates
dup_data = [
    ("Sales", "Alice", 1000),
    ("Sales", "Alice", 1000),
    ("IT", "Charlie", 2000),
    ("IT", "Charlie", 2000),
    ("IT", "Charlie", 2000),
    ("HR", "Eve", 1200),
]

dup_df = spark.createDataFrame(dup_data, ["Department", "Name", "Salary"])

print("DataFrame with duplicates:")
dup_df.show()

# Use row_number to assign sequence number within each partition
dedup_window = Window.partitionBy("Department", "Name", "Salary").orderBy("Department")

dedup_result = dup_df.withColumn(
    "RowNum", row_number().over(dedup_window)
)

print("\n With Row number")
dedup_result.show()

dedup_result = dedup_result.filter("RowNum = 1").drop("RowNum")

print("\nAfter removing duplicates:")
dedup_result.show()