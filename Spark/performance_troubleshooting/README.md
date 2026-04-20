# PySpark Performance Troubleshooting

A hands-on, lesson-by-lesson guide to diagnosing and fixing the most common
PySpark performance problems you will encounter in production.

---

## Learning Path

Run the lessons **in order**. Each one builds on concepts from the previous.

| # | File | Topic |
|---|------|-------|
| 0 | `data_generator.py` | Generate sample datasets (run this first) |
| 1 | `01_explain_and_spark_ui.py` | Read query plans & navigate the Spark UI |
| 2 | `02_partitioning.py` | Partition count, repartition vs coalesce, partition pruning |
| 3 | `03_shuffles_and_joins.py` | Shuffles, join strategies, bucketing |
| 4 | `04_caching.py` | Cache/persist, storage levels, when NOT to cache |
| 5 | `05_data_skew.py` | Detect skew, salting, AQE, split-and-union |
| 6 | `06_udf_vs_builtin.py` | Python UDF vs Pandas UDF vs built-in functions |

---

## Quick Start

```bash
# 1. Install dependencies
pip install pyspark pandas pyarrow faker

# 2. Generate data (creates ./data/ directory)
cd Spark/performance_troubleshooting
python data_generator.py

# 3. Run lessons
python 01_explain_and_spark_ui.py
python 02_partitioning.py
# ...
```

While any script is running, open the **Spark UI** at `http://localhost:4040`.

---

## The Performance Troubleshooting Mental Model

```
Job is slow?
│
├─► Open Spark UI → Jobs → Stages
│     Is one stage much slower than others?
│         YES → click into that stage
│               Are tasks uneven? (one task 10× the median)
│                   YES → Data Skew  →  Lesson 05
│               Is Shuffle Write/Read very high?
│                   YES → Too many shuffles → Lesson 03
│               Is GC Time high?
│                   YES → Too much data in memory → Lesson 04 (check cache)
│
├─► Check the query plan (df.explain("formatted"))
│     See Exchange everywhere?   → Lesson 03 (reduce shuffles)
│     See SortMergeJoin?         → Lesson 03 (try broadcast join)
│     No FileScan predicate?     → Lesson 02 (filter pushdown / partitioning)
│
└─► Check task count / partition count
      Very few tasks (< CPU count)?   → Lesson 02 (too few partitions)
      Thousands of tiny tasks?        → Lesson 02 (too many partitions)
```

---

## Key Configuration Reference

| Config | Default | Tune when |
|--------|---------|-----------|
| `spark.sql.shuffle.partitions` | `200` | Lower for small/local jobs; raise for large shuffles |
| `spark.sql.autoBroadcastJoinThreshold` | `10MB` | Raise if small dims are > 10 MB |
| `spark.sql.adaptive.enabled` | `true` (3.x) | Keep on — handles skew, coalesces shuffle partitions |
| `spark.sql.adaptive.skewJoin.enabled` | `true` (3.x) | Keep on for automatic skew remediation |
| `spark.sql.files.maxPartitionBytes` | `128MB` | Lower for wide/complex rows |
| `spark.sql.execution.arrow.pyspark.enabled` | `true` (3.x) | Required for Pandas UDFs |

---

## Datasets Generated

| File | Rows | Used in |
|------|------|---------|
| `data/orders.parquet` | 5 M | All lessons; **skewed** on `customer_id=1` |
| `data/customers.parquet` | 1 K | Join lessons (small dimension table) |
| `data/events.parquet` | 10 M | Partitioning & caching lessons |
