from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("CatalystExample") \
    .getOrCreate()

# Load employees data
employees = spark.read.csv(
    "/Users/dkulemza/PycharmProjects/mentorship/Spark/data/employees.csv",
    header=True,
    inferSchema=True
)

# Create a departments reference table
departments = spark.createDataFrame([
    ("Engineering", "Tech"),
    ("Product", "Tech"),
    ("Sales", "Revenue"),
    ("Marketing", "Revenue"),
    ("Finance", "Support"),
    ("HR", "Support"),
    ("Legal", "Support"),
    ("Operations", "Support")
], ["department", "category"])

# Example 1: Filter and Join - Find active engineers in Tech departments earning > 100k
result1 = employees.filter(col("status") == "active") \
    .filter(col("department") == "Engineering") \
    .filter(col("salary") > 100000) \
    .join(departments, on="department") \
    .select("employee_id", "first_name", "last_name", "salary", "category")

print("=== Active Engineers earning > 100k ===")
result1.show()

# Example 2: Complex join with aggregation - Avg salary by department category
result2 = employees.join(departments, on="department") \
    .filter(col("status") == "active") \
    .groupBy("category", "department") \
    .agg(
        avg("salary").alias("avg_salary"),
        count("employee_id").alias("employee_count")
    ) \
    .orderBy(col("avg_salary").desc())

print("\n=== Avg Salary by Department Category (Active Employees) ===")
result2.show()

# Example 3: Show Catalyst optimization plan
print("\n=== Catalyst Optimized Plan (Query 1) ===")
result1.explain(extended=True)

print("\n=== Catalyst Optimized Plan (Query 2) ===")
result2.explain(extended=True)

# Example 4: Filter before join (better performance)
result3 = employees.filter(col("years_experience") >= 10) \
    .join(
        departments.filter(col("category") == "Tech"),
        on="department"
    ) \
    .select("employee_id", "first_name", "department", "years_experience", "salary")

print("\n=== Employees with 10+ years in Tech departments ===")
result3.show()

spark.stop()