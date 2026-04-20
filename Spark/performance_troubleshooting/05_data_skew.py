"""
Lesson 05 — Data Skew
======================
GOAL: Detect data skew, understand why it hurts, and fix it.

Key concepts:
  - Data skew = one (or a few) partitions hold far more data than others.
  - In the Spark UI → Stages: one task takes 10× longer than the rest.
  - The job cannot finish faster than its slowest task.
  - Techniques to fix skew:
      1. Salting        — artificially spread a hot key across sub-keys.
      2. Skew join hint — Spark 3.x AQE auto-splits skewed partitions.
      3. Broadcast join — bypass shuffle entirely for the small side.

NOTE: The orders dataset was generated with customer_id=1 owning ~10% of rows.
      That is the "hot key" we will diagnose and fix here.

HOW TO USE:
    python 05_data_skew.py
"""

import time
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (
    SparkSession.builder
    .appName("05_DataSkew")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "8")
    # Disable AQE so we can see the raw skew effect first
    .config("spark.sql.adaptive.enabled", "false")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

orders    = spark.read.parquet("data/orders.parquet")
customers = spark.read.parquet("data/customers.parquet")


# ===========================================================================
# SECTION 1 — Diagnose skew
# ===========================================================================
print("\n" + "=" * 70)
print("SECTION 1 — Diagnosing data skew")
print("=" * 70)
print("""
Symptoms in the Spark UI:
  - Stages tab: one task has 10× the 'Duration' of the median.
  - Tasks detail: 'Shuffle Write Size' is 10× larger for one task.
  - Job hangs at '199/200 tasks complete' for a long time.

First step: check key distribution with a simple aggregation.
""")

key_distribution = (
    orders
    .groupBy("customer_id")
    .count()
    .orderBy(F.desc("count"))
)

print("Top 10 customer_ids by order count:")
key_distribution.show(10)

total = orders.count()
hot_key_rows = orders.filter(F.col("customer_id") == 1).count()
print(f"\nTotal orders        : {total:,}")
print(f"Orders for cust_id=1: {hot_key_rows:,}  ({hot_key_rows/total*100:.1f}% of all data!)")
print("→ customer_id=1 is the hot key causing skew.")


# ===========================================================================
# SECTION 2 — Skewed join (the problem)
# ===========================================================================
print("\n" + "=" * 70)
print("SECTION 2 — Skewed join without fix (slow)")
print("=" * 70)

# AQE disabled — raw skew visible
t0 = time.time()
skewed_join = (
    orders
    .join(customers, on="customer_id", how="inner")
    .groupBy("tier")
    .agg(F.sum("amount").alias("revenue"))
)
skewed_join.show()
print(f"  Skewed join elapsed: {time.time()-t0:.2f}s")
print("  Observe in Spark UI → Stages: one task is much slower than others.")


# ===========================================================================
# SECTION 3 — Fix 1: Salting
# ===========================================================================
print("\n" + "=" * 70)
print("SECTION 3 — Fix: Salting the hot key")
print("=" * 70)
print("""
Salting breaks a single hot key into N sub-keys by appending a random salt.
Both sides of the join are salted identically so matching still works.

Steps:
  1. Add a random salt [0, SALT_FACTOR) to the large table's join key.
  2. Explode the small (broadcast) table to create one row per salt value.
  3. Join on the composite key  (original_key, salt).
  4. The hot key is now spread across SALT_FACTOR partitions.
""")

SALT_FACTOR = 8   # spread the hot key across 8 sub-keys

# Step 1: salt the large table
orders_salted = orders.withColumn(
    "salt", (F.rand() * SALT_FACTOR).cast("int")
).withColumn(
    "customer_id_salted", F.concat_ws("_", F.col("customer_id"), F.col("salt"))
)

# Step 2: explode the small table to replicate each customer SALT_FACTOR times
customers_exploded = (
    customers
    .withColumn("salt_arr", F.array([F.lit(i) for i in range(SALT_FACTOR)]))
    .withColumn("salt", F.explode("salt_arr"))
    .withColumn("customer_id_salted", F.concat_ws("_", F.col("customer_id"), F.col("salt")))
    .drop("salt_arr", "salt")
)

# Step 3: join on the salted key
t0 = time.time()
salted_join = (
    orders_salted
    .join(F.broadcast(customers_exploded), on="customer_id_salted", how="inner")
    .groupBy("tier")
    .agg(F.sum("amount").alias("revenue"))
)
salted_join.show()
print(f"  Salted join elapsed: {time.time()-t0:.2f}s")
print("  Hot key is now split across 8 partitions → tasks are more balanced.")


# ===========================================================================
# SECTION 4 — Fix 2: Adaptive Query Execution (AQE) — Spark 3.x built-in
# ===========================================================================
print("\n" + "=" * 70)
print("SECTION 4 — Fix: Adaptive Query Execution (AQE) skew join")
print("=" * 70)
print("""
Spark 3.x AQE can automatically detect and split skewed partitions at runtime.
Enable it with three config flags:
  spark.sql.adaptive.enabled                        = true  (master switch)
  spark.sql.adaptive.skewJoin.enabled               = true  (skew-join fix)
  spark.sql.adaptive.skewJoin.skewedPartitionFactor = 5     (5× median → skewed)

AQE measures actual partition sizes AFTER the shuffle and splits large ones.
No code change needed — it just works.
""")

spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", str(64 * 1024 * 1024))

t0 = time.time()
aqe_join = (
    orders
    .join(customers, on="customer_id", how="inner")
    .groupBy("tier")
    .agg(F.sum("amount").alias("revenue"))
)
aqe_join.show()
print(f"  AQE join elapsed: {time.time()-t0:.2f}s")
print("  AQE automatically splits the skewed partition — no salting code needed.")


# ===========================================================================
# SECTION 5 — Fix 3: Isolate the hot key + union
# ===========================================================================
print("\n" + "=" * 70)
print("SECTION 5 — Fix: Split-and-union for extreme skew")
print("=" * 70)
print("""
When one key is so large that even salting doesn't help:
  1. Process the hot key separately (broadcast the small side for it).
  2. Process all other keys normally.
  3. Union the results.
""")

spark.conf.set("spark.sql.adaptive.enabled", "false")   # show the manual fix clearly

hot_key_value = 1

# Hot key path: filter both sides, broadcast the small side
hot_orders    = orders.filter(F.col("customer_id") == hot_key_value)
hot_customers = customers.filter(F.col("customer_id") == hot_key_value)
hot_result    = (
    hot_orders
    .join(F.broadcast(hot_customers), on="customer_id", how="inner")
    .groupBy("tier").agg(F.sum("amount").alias("revenue"))
)

# Normal path: exclude the hot key
normal_result = (
    orders.filter(F.col("customer_id") != hot_key_value)
    .join(customers.filter(F.col("customer_id") != hot_key_value), on="customer_id", how="inner")
    .groupBy("tier").agg(F.sum("amount").alias("revenue"))
)

# Combine
final_result = (
    hot_result.union(normal_result)
    .groupBy("tier")
    .agg(F.sum("revenue").alias("revenue"))
)

t0 = time.time()
final_result.show()
print(f"  Split-and-union elapsed: {time.time()-t0:.2f}s")

spark.stop()