from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType

# Initialize Spark Session
spark = SparkSession.builder.appName("CSVReaderExample").getOrCreate()
# sc = spark.sparkContext

# CSV file path
csv_file = "/Users/dkulemza/PycharmProjects/mentorship/Spark/data/employees.csv"

# ===== Reading CSV using RDD =====
# rdd = sc.textFile(csv_file)
# header_rdd = rdd.first()
# data_rdd = rdd.filter(lambda line: line != header_rdd).map(lambda line: line.split(","))

# print("RDD approach:")
# print(data_rdd.take(5))

# ===== Define Schema =====
schema = StructType([
    StructField("employee_id", IntegerType(), True),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("department", StringType(), True),
    StructField("city", StringType(), True),
    StructField("salary", DoubleType(), True),
    StructField("hire_date", DateType(), True),
    StructField("status", StringType(), True),
    StructField("years_experience", IntegerType(), False)
])


# # ===== Reading CSV using DataFrame =====
df = spark.read.option("header", "true").option("inferSchema", "true").schema(schema).csv(csv_file)

print("DataFrame approach:")
df.filter(df.employee_id == 51).show(5)

# df.printSchema()

# Stop Spark Session
spark.stop()