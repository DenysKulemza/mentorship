"""
Lesson 01 — Reading the Query Plan & Spark UI
==============================================
GOAL: Understand what Spark is *actually* doing before chasing performance.

Key concepts:
  - df.explain()  →  physical & logical plan
  - Spark UI (localhost:4040)  →  stages, tasks, shuffle read/write
  - Avoid running queries blind — always read the plan first.

HOW TO USE:
    1. Run `python data_generator.py` first.
    2. Run this file: `python 01_explain_and_spark_ui.py`
    3. While it runs, open http://localhost:4040 → Jobs → Stages
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (
    SparkSession.builder
    .appName("01_ExplainAndSparkUI")
    .master("local[*]")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

orders    = spark.read.parquet("data/orders.parquet")
customers = spark.read.parquet("data/customers.parquet")

# ===========================================================================
# SECTION 1 — explain()  :  reading query plans
# ===========================================================================
print("\n" + "=" * 70)
print("SECTION 1 — explain()")
print("=" * 70)

query = (
    orders
    .filter(F.col("status") == "completed")
    .groupBy("customer_id")
    .agg(F.sum("amount").alias("total_spent"))
    .orderBy(F.desc("total_spent"))
)

spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")

# --- (a) Simple plan (default) ---
print("\n[explain() — simple]")
print("  Reads bottom-up.  The FIRST line is the scan, the LAST is the output.")
query.explain()

# --- (b) Extended plan: logical + physical ---
print("\n[explain('extended') — full logical + physical plan]")
print("  Parsed  → Analyzed → Optimized → Physical")
print("  Look for: Exchange (shuffle), Sort, HashAggregate, FileScan")
query.explain("extended")

# --- (c) Formatted (Spark 3.x) — easiest to read ---
print("\n[explain('formatted') — tree view with metrics]")
query.explain("formatted")

# ===========================================================================
# SECTION 2 — Spotting expensive operators in the plan
# ===========================================================================
print("\n" + "=" * 70)
print("SECTION 2 — What to look for in a plan")
print("=" * 70)

# Exchange → a shuffle is happening  (expensive: network I/O between executors)
# Sort     → data is being sorted    (can be expensive at large scale)
# BroadcastHashJoin  → small table fits in memory, no shuffle needed  (fast!)
# SortMergeJoin      → both sides shuffled and sorted                  (slow!)

join_query = orders.join(customers, on="customer_id", how="inner")

print(f"  Default join row count : {join_query.count():,}")


print("\n[Default join — likely SortMergeJoin (two Exchange nodes)]")
join_query.explain("formatted")

input("\nPress ENTER to close Spark session and exit...")

# Force a broadcast join on the small dimension table
from pyspark.sql.functions import broadcast

join_broadcast = orders.join(broadcast(customers), on="customer_id", how="inner")
print("\n[Broadcast join — BroadcastHashJoin, zero Exchange on customers side]")
join_broadcast.explain("formatted")

# ===========================================================================
# SECTION 3 — Spark UI walkthrough
# ===========================================================================
print("\n" + "=" * 70)
print("SECTION 3 — Spark UI  (http://localhost:4040)")
print("=" * 70)
print("""
While this query runs, open the Spark UI and observe:

Jobs tab:
  - Each .show() / .count() / .write triggers a new Job.

Stages tab (per job):
  - Each stage is a group of tasks separated by a shuffle boundary.
  - Look at 'Shuffle Read' and 'Shuffle Write' columns — high values = pain.
  - Skewed stages: one task takes 10x longer than the median → data skew.

Tasks detail (click a stage):
  - Min / Median / Max task duration — huge spread = skew.
  - GC Time — high GC = too much data in memory per task.
  - Spill (Memory) / Spill (Disk) — executor ran out of memory → disk spill.

SQL tab:
  - Visual DAG of your query plan with real-time metrics per node.
  - 'number of output rows', 'data size' per operator.
""")

# Trigger the queries so you can inspect them live in the UI
print("Running join queries to populate Spark UI...")
print(f"  Default join row count : {join_query.count():,}")
print(f"  Broadcast join row count: {join_broadcast.count():,}")

input("\nPress ENTER to close Spark session and exit...")
spark.stop()