"""
Lesson 06 — Python UDFs vs Built-in Functions
===============================================
GOAL: Understand the performance penalty of Python UDFs and when
      to use Pandas UDFs (vectorised) or native Spark functions instead.

Key concepts:
  - Python UDF (udf)        — row-by-row execution in Python process.
                              Data leaves JVM → Python → back to JVM for each row.
                              Typically 10–100× slower than built-ins.
  - Pandas UDF (pandas_udf) — batch execution via Apache Arrow.
                              No row-by-row serialisation, stays in memory.
                              ~3–10× faster than Python UDF.
  - Built-in Spark functions (F.*)  — run entirely in JVM, no Python overhead.
                              Always the fastest option when available.

Rule: Built-in > Pandas UDF > Python UDF.
      Only reach for UDF when there is NO built-in equivalent.

HOW TO USE:
    python 06_udf_vs_builtin.py
"""

import time
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, DoubleType
from pyspark.sql.functions import pandas_udf
import pandas as pd

spark = (
    SparkSession.builder
    .appName("06_UDF_vs_Builtin")
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
    print(f"  [{label:<35}]  {elapsed:.3f}s  →  {result:,}")


# ===========================================================================
# SECTION 1 — Python UDF (the slow path)
# ===========================================================================
print("\n" + "=" * 70)
print("SECTION 1 — Python UDF  (slowest)")
print("=" * 70)
print("""
How it works:
  1. Spark serialises each JVM Row to Python bytes.
  2. Sends it over a local socket to a Python worker process.
  3. Python calls your function row-by-row.
  4. Result is serialised back to JVM.

Overhead: serialisation + socket + Python interpreter startup.
Visible in Spark UI → Stage → 'Python Time' metric.
""")

# A simple UDF: classify order size
@F.udf(StringType())
def classify_order_python_udf(amount: float) -> str:
    if amount < 100:
        return "small"
    elif amount < 500:
        return "medium"
    else:
        return "large"

orders_with_class_udf = orders.withColumn("size_class", classify_order_python_udf("amount"))
timed("Python UDF   (withColumn)", lambda: orders_with_class_udf.count())


# ===========================================================================
# SECTION 2 — Pandas UDF  (vectorised)
# ===========================================================================
print("\n" + "=" * 70)
print("SECTION 2 — Pandas UDF  (faster via Apache Arrow)")
print("=" * 70)
print("""
How it works:
  1. Spark batches rows into Arrow-format columnar buffers (no row-by-row copy).
  2. Sends the buffer to Python once per batch.
  3. Your function receives a pd.Series, processes the whole batch at once.
  4. Returns a pd.Series — sent back to JVM as a single Arrow buffer.

The same Python logic, ~3–10× less overhead because serialisation is batched.
""")

@pandas_udf(StringType())
def classify_order_pandas_udf(amount: pd.Series) -> pd.Series:
    return pd.cut(
        amount,
        bins=[-float("inf"), 100, 500, float("inf")],
        labels=["small", "medium", "large"]
    ).astype(str)

orders_with_class_pudf = orders.withColumn("size_class", classify_order_pandas_udf("amount"))
timed("Pandas UDF   (withColumn)", lambda: orders_with_class_pudf.count())


# ===========================================================================
# SECTION 3 — Built-in Spark function  (fastest)
# ===========================================================================
print("\n" + "=" * 70)
print("SECTION 3 — Built-in F.*  (fastest — pure JVM)")
print("=" * 70)
print("""
F.when() / F.otherwise() runs entirely in the JVM.
Zero serialisation, no Python process involved.
Catalyst optimizer can also push filters into this expression.
""")

orders_with_class_builtin = orders.withColumn(
    "size_class",
    F.when(F.col("amount") < 100, "small")
     .when(F.col("amount") < 500, "medium")
     .otherwise("large")
)
timed("Built-in F.when (withColumn)", lambda: orders_with_class_builtin.count())


# ===========================================================================
# SECTION 4 — Real-world UDF: complex string processing
# ===========================================================================
print("\n" + "=" * 70)
print("SECTION 4 — Realistic comparison: complex string normalisation")
print("=" * 70)
print("""
Sometimes you genuinely need a UDF for logic that has no built-in equivalent
(e.g. calling an external library, regex with groups, custom scoring).
In those cases, prefer Pandas UDF over Python UDF.
""")

# Simulate a "real" transformation: normalise a free-text status column
import re

@F.udf(StringType())
def normalise_status_python(status: str) -> str:
    """Normalise status string using Python regex (simulates custom logic)."""
    if status is None:
        return "unknown"
    return re.sub(r"[^a-z]", "_", status.lower().strip())

@pandas_udf(StringType())
def normalise_status_pandas(status: pd.Series) -> pd.Series:
    return status.fillna("unknown").str.lower().str.strip().str.replace(r"[^a-z]", "_", regex=True)

# Built-in equivalent
normalise_builtin = (
    F.lower(F.trim(F.col("status")))
)

print("Python UDF normalise_status:")
timed("Python UDF   normalise_status", lambda: orders.withColumn("norm", normalise_status_python("status")).count())

print("\nPandas UDF normalise_status:")
timed("Pandas UDF   normalise_status", lambda: orders.withColumn("norm", normalise_status_pandas("status")).count())

print("\nBuilt-in (lower + trim — no regex custom logic):")
timed("Built-in     lower+trim       ", lambda: orders.withColumn("norm", normalise_builtin).count())


# ===========================================================================
# SECTION 5 — Summary and decision guide
# ===========================================================================
print("\n" + "=" * 70)
print("SECTION 5 — Decision guide")
print("=" * 70)
print("""
  1. Can I do it with F.* built-ins?
        YES → use built-ins. Always fastest.

  2. Does my logic require Python (external lib, complex regex, ML inference)?
        YES → use @pandas_udf. Vectorised, Arrow-backed, much faster than row UDF.

  3. I absolutely must use a Python UDF (legacy code, complex state):
        → Cache the input DataFrame first so the UDF overhead is paid only once.
        → Keep the UDF simple — avoid I/O, imports inside the function.
        → Profile in Spark UI: look for 'Python Time' dominating task time.

Key settings for Pandas UDF performance:
  spark.sql.execution.arrow.pyspark.enabled = true   (enabled by default in 3.x)
  spark.sql.execution.arrow.maxRecordsPerBatch       (default 10000, tune per RAM)
""")

spark.stop()
