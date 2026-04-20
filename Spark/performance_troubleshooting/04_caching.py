"""
Lesson 04 — Caching & Persistence
===================================
GOAL: Know when to cache, which storage level to choose,
      and — just as importantly — when NOT to cache.

Key concepts:
  - Spark is lazy: every action re-executes the full lineage by default.
  - cache() / persist() materialises the result so it is reused.
  - Wrong caching wastes memory and can be slower than not caching at all.
  - Always unpersist() when you are done.

Storage levels:
  MEMORY_ONLY          — fastest, OOM risk for large data
  MEMORY_AND_DISK      — spills to disk when memory full  (safe default)
  DISK_ONLY            — always on disk, slowest
  MEMORY_ONLY_SER      — serialized in memory  (smaller footprint)
  MEMORY_AND_DISK_SER  — serialized, spills to disk

HOW TO USE:
    python 04_caching.py
"""

import time
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.storagelevel import StorageLevel

spark = (
    SparkSession.builder
    .appName("04_Caching")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

orders = spark.read.parquet("data/orders.parquet")


def timed(label: str, fn):
    t0 = time.time()
    result = fn()
    elapsed = time.time() - t0
    print(f"  [{label}]  {elapsed:.2f}s  →  {result:,}")


# ===========================================================================
# SECTION 1 — Repeated computation without caching
# ===========================================================================
print("\n" + "=" * 70)
print("SECTION 1 — Without caching: lineage re-executed on each action")
print("=" * 70)
print("""
Each .count() re-reads the Parquet file and re-runs all transformations.
Expensive when the transformation chain is heavy or data is remote (S3).
""")

heavy_transform = (
    orders
    .filter(F.col("status") == "completed")
    .withColumn("discounted", F.col("amount") * 0.9)
    .groupBy("customer_id")
    .agg(
        F.sum("discounted").alias("total_discounted"),
        F.count("*").alias("order_count")
    )
)

print("Running the same computation 3× WITHOUT cache:")
timed("run 1 (no cache)", heavy_transform.count)
timed("run 2 (no cache)", heavy_transform.count)
timed("run 3 (no cache)", heavy_transform.count)

input("\nPress ENTER to close Spark session and exit...")


# ===========================================================================
# SECTION 2 — Caching the result
# ===========================================================================
print("\n" + "=" * 70)
print("SECTION 2 — With caching: computed once, served from memory")
print("=" * 70)

heavy_transform.cache()  # equivalent to persist(StorageLevel.MEMORY_AND_DISK)

print("Running the same computation 3× WITH cache:")
timed("run 1 (triggers materialisation)", heavy_transform.count)  # first run caches
timed("run 2 (served from cache)",        heavy_transform.count)
timed("run 3 (served from cache)",        heavy_transform.count)

# Always release the cache when you are done — do not leave stale data in memory
heavy_transform.unpersist()
print("\n[unpersist() called — memory freed]")

input("\nPress ENTER to close Spark session and exit...")


# ===========================================================================
# SECTION 3 — Choosing the right storage level
# ===========================================================================
print("\n" + "=" * 70)
print("SECTION 3 — Storage levels")
print("=" * 70)
print("""
Use MEMORY_ONLY when:
  - Dataset fits comfortably in heap (< 40% executor memory).
  - You need the fastest possible re-reads.

Use MEMORY_AND_DISK when:
  - Dataset size is uncertain or close to heap limit.
  - Spill to disk is acceptable vs. recomputing from source.

Use DISK_ONLY when:
  - Dataset is large, re-reading source is expensive (remote S3).
  - You do not need sub-second latency on re-reads.

Use MEMORY_ONLY_SER / MEMORY_AND_DISK_SER when:
  - Memory is tight; serialised objects are ~2-5x smaller than JVM objects.
  - Trade CPU (serialisation) for memory.
""")

events = spark.read.parquet("data/events.parquet")

events.persist(StorageLevel.MEMORY_AND_DISK)

print("Persisting events with MEMORY_AND_DISK:")
timed("first access  (materialises cache)", lambda: events.count())
timed("second access (from cache)",         lambda: events.count())

events.unpersist()

input("\nPress ENTER to close Spark session and exit...")



# ===========================================================================
# SECTION 4 — When NOT to cache
# ===========================================================================
print("\n" + "=" * 70)
print("SECTION 4 — When NOT to cache")
print("=" * 70)
print("""
DO cache when:
  ✓ The same DataFrame is referenced in multiple downstream actions.
  ✓ The computation is expensive (joins, aggregations, UDFs).
  ✓ You are iterating (ML training loops, graph algorithms).

DO NOT cache when:
  ✗ The DataFrame is used only once — caching wastes memory + serialisation time.
  ✗ The dataset is larger than available executor memory and DISK_ONLY is slow
    — it may be faster to just recompute from source.
  ✗ The source is already fast (local SSD, warm Alluxio tier).
  ✗ You are caching an intermediate step before a wide transformation that will
    shuffle anyway — cache the POST-shuffle result instead.

Spot bad caches in the Spark UI → Storage tab:
  - If 'Fraction Cached' is low, data is spilling to disk or being evicted.
  - If a cached dataset is used only once, remove the cache() call.
""")

# Example of pointless caching (used only once)
pointless = orders.filter(F.col("status") == "pending").cache()
result = pointless.count()       # only used once — cache was wasteful
pointless.unpersist()
print(f"Pointless cache example completed. result={result:,}  (cache had no benefit)")

input("\nPress ENTER to close Spark session and exit...")


spark.stop()