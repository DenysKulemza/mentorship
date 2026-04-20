"""
Lesson 03 — Shuffles & Join Strategies
=======================================
GOAL: Know when Spark shuffles data, why shuffles are expensive,
      and how to pick the right join strategy to minimize them.

Key concepts:
  - Shuffle = moving data across the network between executors.
  - Wide transformations  (groupBy, join, distinct, repartition) cause shuffles.
  - Narrow transformations (filter, select, map)               do NOT shuffle.
  - Join strategies:
      BroadcastHashJoin  — small table fits in executor memory  (no shuffle)
      SortMergeJoin      — both sides shuffled + sorted          (default)
      ShuffledHashJoin   — one side hashed, both shuffled        (rare)
  - spark.sql.autoBroadcastJoinThreshold (default 10 MB) controls auto-broadcast.

HOW TO USE:
    python 03_shuffles_and_joins.py
"""

import time
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (
    SparkSession.builder
    .appName("03_ShufflesAndJoins")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

orders    = spark.read.parquet("data/orders.parquet")
customers = spark.read.parquet("data/customers.parquet")


# ===========================================================================
# SECTION 1 — Narrow vs Wide transformations
# ===========================================================================
print("\n" + "=" * 70)
print("SECTION 1 — Narrow vs Wide transformations")
print("=" * 70)
print("""
NARROW  (no shuffle, data stays in the same partition):
  filter()  select()  withColumn()  map()  flatMap()  union()

WIDE  (shuffle required, data moves across partitions/executors):
  groupBy()  agg()  join()  distinct()  orderBy()  repartition()

Rule: minimize wide transformations. Chain filters/selects BEFORE a groupBy.
""")

# Bad: groupBy first, then filter (shuffles more data)
bad_plan = orders.groupBy("customer_id").agg(F.sum("amount").alias("total")).filter(F.col("total") > 1000)
print("[BAD plan — groupBy before filter]")
bad_plan.explain("formatted")

# Good: filter first, then groupBy (shuffle carries less data)
good_plan = orders.filter(F.col("status") == "completed").groupBy("customer_id").agg(F.sum("amount").alias("total"))
print("[GOOD plan — filter before groupBy (predicate pushdown)]")
good_plan.explain("formatted")


# ===========================================================================
# SECTION 2 — SortMergeJoin (default for large-large joins)
# ===========================================================================
print("\n" + "=" * 70)
print("SECTION 2 — SortMergeJoin (default large-table join)")
print("=" * 70)
print("""
When both tables are large, Spark uses SortMergeJoin:
  1. Both sides are shuffled on the join key.
  2. Both sides are sorted on the join key.
  3. Rows with matching keys are merged.

Cost: 2 shuffles (one per side) + 2 sorts.  Shows as 2 'Exchange' nodes in plan.
""")

# Disable auto-broadcast so we force SortMergeJoin even on the small table
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")

smj = orders.join(customers, on="customer_id", how="inner")
print("[SortMergeJoin — autoBroadcast disabled]")
smj.explain("formatted")

t0 = time.time()
print(f"  SortMergeJoin count: {smj.count():,}   ({time.time()-t0:.2f}s)")
input("\nPress ENTER to close Spark session and exit...")


# ===========================================================================
# SECTION 3 — BroadcastHashJoin (best for small dimension tables)
# ===========================================================================
print("\n" + "=" * 70)
print("SECTION 3 — BroadcastHashJoin (best for small-table joins)")
print("=" * 70)
print("""
BroadcastHashJoin sends the SMALL table to every executor in memory.
No shuffle needed for the large table — it stays where it is.

When to use:
  - One side is small (fits in executor memory, typically < 100 MB).
  - Use hint:  df.join(broadcast(small_df), ...)
  - Or set:    spark.sql.autoBroadcastJoinThreshold = "50mb"
""")

# Re-enable auto-broadcast (threshold 50 MB)
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", str(50 * 1024 * 1024))

bhj_auto = orders.join(customers, on="customer_id", how="inner")
print("[BroadcastHashJoin — auto threshold 50MB]")
bhj_auto.explain("formatted")

t0 = time.time()
print(f"  BroadcastHashJoin count: {bhj_auto.count():,}   ({time.time()-t0:.2f}s)")

# Explicit broadcast hint (preferred — self-documenting)
bhj_hint = orders.join(F.broadcast(customers), on="customer_id", how="inner")
print("\n[BroadcastHashJoin — explicit broadcast() hint]")
bhj_hint.explain("formatted")

input("\nPress ENTER to close Spark session and exit...")


# ===========================================================================
# SECTION 4 — Bucket joins  (eliminate shuffle for repeated large joins)
# ===========================================================================
print("\n" + "=" * 70)
print("SECTION 4 — Bucketing  (pre-shuffle data at write time)")
print("=" * 70)
print("""
Bucketing pre-partitions data by a join/group key when writing to disk.
If two tables are bucketed on the same key with the same bucket count,
Spark can skip the shuffle entirely on subsequent joins — huge wins on
pipelines that repeatedly join on the same key.

Cost: one-time shuffle at write time.
Benefit: every future join/groupBy on that key is shuffle-free.
""")

# Write orders bucketed by customer_id into 8 buckets
print("Writing bucketed orders table...")
(
    orders
    .write
    .mode("overwrite")
    .bucketBy(8, "customer_id")
    .sortBy("customer_id")
    .saveAsTable("orders_bucketed")
)

# Write customers bucketed by the same key
print("Writing bucketed customers table...")
(
    customers
    .write
    .mode("overwrite")
    .bucketBy(8, "customer_id")
    .sortBy("customer_id")
    .saveAsTable("customers_bucketed")
)

# Join bucketed tables — no Exchange nodes on either side!
orders_b    = spark.table("orders_bucketed")
customers_b = spark.table("customers_bucketed")

spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")  # force SMJ path
bucketed_join = orders_b.join(customers_b, on="customer_id", how="inner")
print("\n[Bucketed join — should show NO Exchange for the join shuffle]")
bucketed_join.explain("formatted")

t0 = time.time()
print(f"  Bucketed join count: {bucketed_join.count():,}   ({time.time()-t0:.2f}s)")


input("\nPress ENTER to close Spark session and exit...")

spark.stop()