"""
Data Generator for Performance Troubleshooting Examples
========================================================
Generates synthetic datasets used across all performance lessons.
Run this first before running any other scripts in this module.

Usage:
    python data_generator.py
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

spark = (
    SparkSession.builder
    .appName("DataGenerator")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "8")  # Keep low for local dev
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")


# ---------------------------------------------------------------------------
# 1. Orders dataset  (~5M rows, used in join & skew lessons)
# ---------------------------------------------------------------------------
def generate_orders(num_rows: int = 100_000_000) -> None:
    """
    Simulates an e-commerce orders table.
    Intentionally skewed: customer_id 1 owns ~40% of all orders (skew lesson).
    """
    # Normal orders
    normal_orders = (
        spark.range(num_rows)
        .withColumn("order_id", F.col("id").cast(IntegerType()))
        .withColumn(
            "customer_id",
            # 90% of rows get a customer_id between 2 and 1000
            F.when(F.rand() > 0.1, (F.rand() * 999 + 2).cast(IntegerType()))
            # 10% of rows go to customer_id=1  → artificial skew
            .otherwise(F.lit(1))
        )
        .withColumn("product_id", (F.rand() * 500 + 1).cast(IntegerType()))
        .withColumn("amount", F.round(F.rand() * 990 + 10, 2))
        .withColumn("status",
                    F.element_at(F.array(F.lit("completed"), F.lit("pending"), F.lit("cancelled")),
                                 (F.rand() * 3 + 1).cast(IntegerType())))
        .drop("id")
    )
    normal_orders.write.mode("overwrite").parquet("data/orders.parquet")
    print(f"[OK] orders.parquet written ({num_rows:,} rows)")


# ---------------------------------------------------------------------------
# 2. Customers dataset  (~1 000 rows, used in join lessons)
# ---------------------------------------------------------------------------
def generate_customers(num_rows: int = 50_000_000) -> None:
    """Small dimension table — perfect candidate for a broadcast join."""
    customers = (
        spark.range(num_rows)
        .withColumn("customer_id", (F.col("id") + 1).cast(IntegerType()))
        .withColumn("name", F.concat(F.lit("Customer_"), F.col("customer_id")))
        .withColumn("country",
                    F.element_at(F.array(F.lit("US"), F.lit("UK"), F.lit("DE"), F.lit("FR"), F.lit("CA")),
                                 (F.rand() * 5 + 1).cast(IntegerType())))
        .withColumn("tier",
                    F.element_at(F.array(F.lit("bronze"), F.lit("silver"), F.lit("gold")),
                                 (F.rand() * 3 + 1).cast(IntegerType())))
        .drop("id")
    )
    customers.write.mode("overwrite").parquet("data/customers.parquet")
    print(f"[OK] customers.parquet written ({num_rows:,} rows)")


# ---------------------------------------------------------------------------
# 3. Events dataset  (~10M rows, used in partitioning & caching lessons)
# ---------------------------------------------------------------------------
def generate_events(num_rows: int = 10_000_000) -> None:
    """
    Simulates a clickstream / event log.
    Has a 'date' column useful for partition pruning demonstrations.
    """
    events = (
        spark.range(num_rows)
        .withColumn("event_id", F.col("id").cast(IntegerType()))
        .withColumn("user_id", (F.rand() * 50_000 + 1).cast(IntegerType()))
        .withColumn("event_type",
                    F.element_at(F.array(F.lit("click"), F.lit("view"), F.lit("purchase"), F.lit("scroll")),
                                 (F.rand() * 4 + 1).cast(IntegerType())))
        .withColumn(
            "event_date",
            F.date_add(F.lit("2024-01-01"), (F.rand() * 364).cast(IntegerType()))
        )
        .withColumn("session_duration_sec", (F.rand() * 600).cast(IntegerType()))
        .drop("id")
    )
    events.write.mode("overwrite").parquet("data/events.parquet")
    print(f"[OK] events.parquet written ({num_rows:,} rows)")


if __name__ == "__main__":
    import os
    os.makedirs("data", exist_ok=True)

    print("Generating datasets (this may take a minute)...")
    generate_customers(num_rows=10_000_000)
    generate_orders(num_rows=30_000_000)
    generate_events(num_rows=20_000_000)
    print("\nAll datasets ready. Open http://localhost:4040 to inspect the Spark UI.")

    spark.stop()
