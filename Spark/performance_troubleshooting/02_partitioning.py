"""
Lesson 02 — Partitioning
========================
GOAL: Understand how partition count affects parallelism, shuffle size,
      and task duration.  Learn when to repartition vs coalesce.

Key concepts:
  - A partition = a unit of work for one task on one executor core.
  - Too few partitions  → cores sit idle, tasks are huge, risk of OOM.
  - Too many partitions → scheduling overhead, tiny tasks, driver bottleneck.
  - Rule of thumb      → 2–4 partitions per CPU core.
  - spark.sql.shuffle.partitions (default 200) controls post-shuffle count.

HOW TO USE:
    python 02_partitioning.py
"""

import time
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (
    SparkSession.builder
    .appName("02_Partitioning")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "200")   # default
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

orders = spark.read.parquet("data/orders.parquet")
events = spark.read.parquet("data/events.parquet")


def timed(label: str, fn):
    """Utility: run fn(), print label + elapsed time."""
    t0 = time.time()
    result = fn()
    elapsed = time.time() - t0
    print(f"  [{label}]  elapsed={elapsed:.2f}s  result={result:,}")
    return result


# ===========================================================================
# SECTION 1 — Checking current partition count
# ===========================================================================
print("\n" + "=" * 70)
print("SECTION 1 — Inspect partition count after read")
print("=" * 70)

print(f"\norders  partitions after read : {orders.rdd.getNumPartitions()}")
print(f"events  partitions after read : {events.rdd.getNumPartitions()}")
# Spark determines read-time partitions from file sizes and
# spark.sql.files.maxPartitionBytes (default 128 MB).


# ===========================================================================
# SECTION 2 — Too few vs too many partitions
# ===========================================================================
print("\n" + "=" * 70)
print("SECTION 2 — Effect of partition count on aggregation speed")
print("=" * 70)

def run_agg(df):
    return (
        df.groupBy("customer_id")
          .agg(F.sum("amount").alias("total"))
          .count()
    )

# Coalesce to a single partition — forces everything onto one core (bad)
orders_1 = orders.coalesce(1)
print(f"\nCoalesced to 1 partition:")
timed("  1 partition ", lambda: run_agg(orders_1))

# Repartition to match available cores (good)
import os
num_cores = os.cpu_count() or 4
orders_balanced = orders.repartition(num_cores * 2)
print(f"\nRepartitioned to {num_cores * 2} partitions (2x cpu count = {num_cores}):")
timed(f"  {num_cores * 2} partitions", lambda: run_agg(orders_balanced))

# Default 200 shuffle partitions — often too many for local/small clusters
# but right-sized for a large cluster
print(f"\nDefault 200 shuffle partitions (after groupBy):")
timed("  200 partitions", lambda: run_agg(orders))


# ===========================================================================
# SECTION 3 — repartition() vs coalesce()
# ===========================================================================
print("\n" + "=" * 70)
print("SECTION 3 — repartition() vs coalesce()")
print("=" * 70)
print("""
repartition(n):
  - Full shuffle — redistributes data evenly across n partitions.
  - Use when: increasing partition count, or need even distribution.
  - Cost: triggers a shuffle (expensive).

coalesce(n):
  - No shuffle — merges existing partitions locally on each executor.
  - Use when: reducing partition count (e.g. before writing to disk).
  - Cost: may produce uneven partitions (some tasks do more work).
  - NEVER use coalesce(1) before a wide transformation — it kills parallelism.
""")

# Demonstrate: coalesce before write avoids tiny files problem
print("Writing with 200 partitions → 200 small Parquet files (bad)")
t0 = time.time()
events.write.mode("overwrite").parquet("/Users/dkulemza/PycharmProjects/mentorship/Spark/performance_troubleshooting/events_many_files.parquet")
print(f"  Done in {time.time()-t0:.2f}s")

print("\nWriting with coalesce(8) → 8 reasonably sized files (better)")
t0 = time.time()
events.coalesce(8).write.mode("overwrite").parquet("/Users/dkulemza/PycharmProjects/mentorship/Spark/performance_troubleshooting/events_few_files.parquet")
print(f"  Done in {time.time()-t0:.2f}s")

import subprocess
many = subprocess.getoutput("ls /tmp/events_many_files.parquet/*.parquet | wc -l").strip()
few  = subprocess.getoutput("ls /tmp/events_few_files.parquet/*.parquet | wc -l").strip()
print(f"\n  many_files partition count : {many}")
print(f"  few_files  partition count : {few}")


# ===========================================================================
# SECTION 4 — Partition pruning  (read only what you need)
# ===========================================================================
print("\n" + "=" * 70)
print("SECTION 4 — Partition pruning on partitioned Parquet")
print("=" * 70)
print("""
Writing data partitioned by a column (e.g. event_date) lets Spark skip
irrelevant data files entirely at read time — zero I/O for filtered partitions.
""")

# Write events partitioned by month (derived from event_date)
events_with_month = events.withColumn("month", F.date_format("event_date", "yyyy-MM"))

print("Writing events partitioned by month...")
events_with_month.write.mode("overwrite").partitionBy("month").parquet("/Users/dkulemza/PycharmProjects/mentorship/Spark/performance_troubleshooting/events_by_month.parquet")

# Read with filter — Spark only opens the matching month directories
print("\n[Without partition filter — scans ALL months]")
df_all = spark.read.parquet("/Users/dkulemza/PycharmProjects/mentorship/Spark/performance_troubleshooting/events_by_month.parquet")
df_all.explain("formatted")   # look for 'PartitionFilters: []'

print("\n[With partition filter — scans ONE month only]")
df_jan = spark.read.parquet("/Users/dkulemza/PycharmProjects/mentorship/Spark/performance_troubleshooting/events_by_month.parquet").filter(F.col("month") == "2024-01")
df_jan.explain("formatted")   # look for 'PartitionFilters: [isnotnull(month...) ... ]'

t0 = time.time(); cnt = df_all.count(); print(f"  All months count : {cnt:,}  ({time.time()-t0:.2f}s)")
t0 = time.time(); cnt = df_jan.count(); print(f"  Jan only   count : {cnt:,}  ({time.time()-t0:.2f}s)")
print("Notice: January-only scan is much faster because Spark skips other directories.")

spark.stop()