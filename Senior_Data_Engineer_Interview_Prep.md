# 🎯 Senior Data Engineer Interview Preparation Guide
## For 5+ Years Experience | Spark | SQL | Databricks

---

## 📋 Table of Contents
1. [Apache Spark - Advanced Concepts](#apache-spark-advanced)
2. [SQL - Advanced Interview Questions](#sql-advanced)
3. [Databricks Specific Questions](#databricks)
4. [PySpark Optimization & Performance](#pyspark-optimization)
5. [Real-World Scenarios](#real-world-scenarios)
6. [System Design for Data Engineering](#system-design)
7. [Practice Projects](#practice-projects)

---

# 🔥 Apache Spark - Advanced Concepts

## 1. Spark Architecture & Internals

### Q1: Explain Spark's execution model in detail (DAG, Stages, Tasks)
**Answer:**
- **DAG (Directed Acyclic Graph)**: When you submit a Spark job, Spark creates a DAG of stages
- **Stages**: DAG is divided into stages at shuffle boundaries
- **Tasks**: Each stage contains multiple tasks, one task per partition
- **Execution Flow**:
  1. User code → Logical Plan
  2. Logical Plan → Physical Plan (Catalyst Optimizer)
  3. Physical Plan → DAG of Stages
  4. Stages → Tasks executed on executors

**Example:**
```python
# This creates a DAG with multiple stages
df = spark.read.parquet("data/")
df2 = df.groupBy("category").agg(sum("amount"))  # Stage 1: Read + Shuffle
df3 = df2.filter(col("sum(amount)") > 1000)      # Stage 2: Filter
df3.write.parquet("output/")                      # Stage 3: Write
```

### Q2: What is Catalyst Optimizer? Explain its phases.
**Answer:**
Catalyst is Spark's query optimizer with 4 phases:

1. **Analysis**: Resolves references, validates schema
2. **Logical Optimization**: 
   - Predicate pushdown
   - Constant folding
   - Column pruning
3. **Physical Planning**: Creates multiple physical plans
4. **Code Generation**: Generates optimized Java bytecode using Tungsten

**Example of Predicate Pushdown:**
```python
# Original query
df = spark.read.parquet("large_table")
df_filtered = df.filter(col("date") == "2024-01-01")

# Catalyst pushes filter to read operation (predicate pushdown)
# Only reads data for date='2024-01-01' from Parquet files
```

### Q3: Explain Spark's Memory Management (On-Heap vs Off-Heap)
**Answer:**
- **On-Heap Memory**: Managed by JVM garbage collector
  - Storage Memory: Caching, broadcasting
  - Execution Memory: Shuffles, joins, sorts
  - User Memory: User data structures
  - Reserved Memory: Spark internals (300MB)

- **Off-Heap Memory**: Direct memory outside JVM
  - Better performance, no GC overhead
  - Configured using `spark.memory.offHeap.enabled`

**Memory Calculation:**
```
Total Executor Memory = executor-memory
Usable Memory = Total - Reserved (300MB)
Storage Memory = Usable × spark.memory.fraction (0.6) × spark.memory.storageFraction (0.5)
Execution Memory = Usable × spark.memory.fraction (0.6) × (1 - spark.memory.storageFraction)
```

### Q4: What is Adaptive Query Execution (AQE)? Benefits?
**Answer:**
AQE is a runtime optimization feature in Spark 3.0+ that reoptimizes query plans based on runtime statistics.

**Key Features:**
1. **Dynamic Partition Coalescing**: Reduces number of partitions after shuffle
2. **Dynamic Join Strategy**: Switches from SortMergeJoin to BroadcastJoin
3. **Skew Join Optimization**: Splits skewed partitions

**Configuration:**
```python
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
```

### Q5: Explain different types of Joins in Spark and when to use each
**Answer:**

| Join Type | Use Case | Performance |
|-----------|----------|-------------|
| **Broadcast Join** | Small table (< 10MB) + Large table | Fastest, no shuffle |
| **Sort-Merge Join** | Large + Large tables | Shuffle required |
| **Shuffle Hash Join** | Medium sized tables | Less memory than Sort-Merge |
| **Cartesian Join** | No join condition | Avoid in production |

**Example:**
```python
# Force Broadcast Join
from pyspark.sql.functions import broadcast
result = large_df.join(broadcast(small_df), "key")

# Sort-Merge Join (default for large tables)
result = large_df1.join(large_df2, "key")

# Check join strategy
result.explain()
```

### Q6: What causes Data Skew? How to handle it?
**Answer:**
**Causes:**
- Uneven distribution of data across partitions
- Hot keys in join operations
- Improper partitioning

**Solutions:**

```python
# 1. Salting Technique for Skewed Joins
from pyspark.sql.functions import rand, concat, lit

# Add salt to skewed key
skewed_df = skewed_df.withColumn("salt", (rand() * 10).cast("int"))
skewed_df = skewed_df.withColumn("salted_key", concat(col("key"), lit("_"), col("salt")))

# Explode the other dataframe
normal_df = normal_df.withColumn("salt", explode(array([lit(i) for i in range(10)])))
normal_df = normal_df.withColumn("salted_key", concat(col("key"), lit("_"), col("salt")))

# Join on salted key
result = skewed_df.join(normal_df, "salted_key")

# 2. Adaptive Query Execution (Spark 3.0+)
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "256MB")

# 3. Repartition with custom logic
df.repartition(200, "key")
```

### Q7: Explain Shuffle Operations and How to Optimize Them
**Answer:**
**Shuffle Operations:** Data redistribution across partitions
- groupBy, join, distinct, repartition, coalesce

**Optimization Strategies:**

```python
# 1. Reduce shuffle data
df.select("col1", "col2")  # Column pruning
  .filter(col("date") >= "2024-01-01")  # Predicate pushdown
  .groupBy("col1").sum("col2")

# 2. Tune shuffle partitions
spark.conf.set("spark.sql.shuffle.partitions", "200")  # Default is 200

# Calculate optimal partitions: target_size / partition_size
# If data is 10GB and target partition is 128MB:
# optimal_partitions = 10000/128 ≈ 80

# 3. Use coalesce instead of repartition when reducing partitions
df.coalesce(10)  # No shuffle
df.repartition(10)  # Full shuffle

# 4. Persist before multiple shuffles
df.cache()
df.groupBy("col1").count()
df.groupBy("col2").count()
```

### Q8: What is Tungsten Execution Engine?
**Answer:**
Tungsten is Spark's physical execution engine for performance optimization:

**Key Features:**
1. **Whole-Stage Code Generation**: Combines operators into single Java function
2. **Cache-Aware Computation**: Optimizes memory access patterns
3. **Off-Heap Memory Management**: Reduces GC overhead
4. **Compact Binary Format**: Reduces memory footprint

**Example:**
```python
# Whole-stage codegen example
df.filter(col("age") > 25)
  .select("name", "salary")
  .filter(col("salary") > 50000)

# Tungsten generates single function instead of 3 separate operations
# Check if codegen is used:
df.explain(mode="formatted")  # Look for WholeStageCodegen
```

### Q9: Explain Spark Partitioning Strategies
**Answer:**

```python
# 1. Hash Partitioning (default)
df.repartition(100, "user_id")  # Data with same user_id goes to same partition

# 2. Range Partitioning
df.repartitionByRange(100, "age")  # Sorted partitions

# 3. Custom Partitioning
from pyspark.sql.functions import year
df.repartition(year("date"))  # Partition by year

# 4. Check current partitions
print(f"Number of partitions: {df.rdd.getNumPartitions()}")

# 5. Partition discovery (file-based)
# Data organized as: /data/year=2024/month=01/
df = spark.read.parquet("/data/")
# Spark automatically discovers partitions
```

### Q10: What is the difference between cache() and persist()?
**Answer:**

```python
from pyspark import StorageLevel

# cache() - stores in memory only (default: MEMORY_ONLY)
df.cache()

# persist() - allows storage level customization
df.persist(StorageLevel.MEMORY_AND_DISK)  # Spill to disk if memory full
df.persist(StorageLevel.MEMORY_ONLY_SER)  # Serialized in memory
df.persist(StorageLevel.DISK_ONLY)        # Only on disk
df.persist(StorageLevel.OFF_HEAP)         # Off-heap memory

# Best Practices:
# 1. Use cache() for iterative algorithms
# 2. Use MEMORY_AND_DISK for large datasets
# 3. Unpersist when done
df.unpersist()

# Check cached data
spark.catalog.listTables()
spark.catalog.isCached("table_name")
```

---

## 2. Advanced PySpark Transformations

### Q11: Explain Window Functions with Complex Examples
**Answer:**

```python
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, rank, dense_rank, lag, lead, sum, avg

# Sample data
data = [
    ("Sales", "John", 5000, "2024-01"),
    ("Sales", "Jane", 6000, "2024-01"),
    ("IT", "Bob", 7000, "2024-01"),
    ("Sales", "John", 5500, "2024-02"),
]
df = spark.createDataFrame(data, ["dept", "name", "salary", "month"])

# 1. Ranking within department
window_spec = Window.partitionBy("dept").orderBy(col("salary").desc())
df.withColumn("rank", rank().over(window_spec))

# 2. Running total
window_spec = Window.partitionBy("dept").orderBy("month").rowsBetween(Window.unboundedPreceding, Window.currentRow)
df.withColumn("running_total", sum("salary").over(window_spec))

# 3. Compare with previous month
window_spec = Window.partitionBy("name").orderBy("month")
df.withColumn("prev_month_salary", lag("salary", 1).over(window_spec))
  .withColumn("salary_change", col("salary") - col("prev_month_salary"))

# 4. Moving average (3 months)
window_spec = Window.partitionBy("name").orderBy("month").rowsBetween(-2, 0)
df.withColumn("moving_avg_3m", avg("salary").over(window_spec))

# 5. Percent of total by department
window_spec = Window.partitionBy("dept")
df.withColumn("dept_total", sum("salary").over(window_spec))
  .withColumn("percent_of_dept", (col("salary") / col("dept_total")) * 100)
```

### Q12: Complex UDFs and Performance Implications
**Answer:**

```python
from pyspark.sql.functions import udf, pandas_udf
from pyspark.sql.types import StringType, IntegerType
import pandas as pd

# ❌ BAD: Regular Python UDF (slow, serialization overhead)
@udf(returnType=StringType())
def categorize_age(age):
    if age < 18:
        return "Minor"
    elif age < 60:
        return "Adult"
    else:
        return "Senior"

df.withColumn("category", categorize_age(col("age")))

# ✅ GOOD: Pandas UDF (vectorized, faster)
@pandas_udf(StringType())
def categorize_age_pandas(age: pd.Series) -> pd.Series:
    return pd.cut(age, bins=[0, 18, 60, 100], labels=["Minor", "Adult", "Senior"])

df.withColumn("category", categorize_age_pandas(col("age")))

# ✅ BETTER: Use native Spark functions
from pyspark.sql.functions import when
df.withColumn("category", 
    when(col("age") < 18, "Minor")
    .when(col("age") < 60, "Adult")
    .otherwise("Senior"))

# Complex Pandas UDF Example
@pandas_udf(IntegerType())
def complex_calculation(price: pd.Series, quantity: pd.Series, discount: pd.Series) -> pd.Series:
    total = price * quantity
    return (total * (1 - discount)).astype(int)

df.withColumn("final_amount", complex_calculation(col("price"), col("quantity"), col("discount")))
```

### Q13: Handle Complex Nested JSON Structures
**Answer:**

```python
from pyspark.sql.functions import explode, col, from_json, get_json_object

# Sample nested JSON
data = '''
{
  "order_id": "001",
  "customer": {
    "name": "John",
    "address": {
      "city": "NYC",
      "zip": "10001"
    }
  },
  "items": [
    {"product": "Laptop", "price": 1000},
    {"product": "Mouse", "price": 20}
  ]
}
'''

# Read JSON
df = spark.read.json(sc.parallelize([data]))

# 1. Access nested fields
df.select(
    col("order_id"),
    col("customer.name").alias("customer_name"),
    col("customer.address.city").alias("city")
)

# 2. Explode arrays
df.select(
    col("order_id"),
    explode(col("items")).alias("item")
).select(
    "order_id",
    "item.product",
    "item.price"
)

# 3. Parse JSON string column
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

schema = StructType([
    StructField("product", StringType()),
    StructField("price", IntegerType())
])

df.withColumn("parsed", from_json(col("json_string"), schema))

# 4. Flatten nested structure
df.select(
    "order_id",
    col("customer.name").alias("customer_name"),
    col("customer.address.*")  # Flatten all address fields
)

# 5. Handle array of structs
df.select(
    "order_id",
    explode("items").alias("item")
).selectExpr(
    "order_id",
    "item.product as product",
    "item.price as price"
)
```

### Q14: Advanced DataFrame Joins and Optimizations
**Answer:**

```python
# 1. Broadcast Join for small dimension tables
from pyspark.sql.functions import broadcast

# Small dimension table < 10MB
dim_df = spark.read.parquet("dimensions")
fact_df = spark.read.parquet("facts")

result = fact_df.join(broadcast(dim_df), "key")

# 2. Bucket Join (avoid shuffle)
# Pre-bucket tables during write
fact_df.write.bucketBy(100, "key").sortBy("key").saveAsTable("fact_bucketed")
dim_df.write.bucketBy(100, "key").sortBy("key").saveAsTable("dim_bucketed")

# Join bucketed tables (no shuffle!)
fact_bucketed = spark.table("fact_bucketed")
dim_bucketed = spark.table("dim_bucketed")
result = fact_bucketed.join(dim_bucketed, "key")

# 3. Range Join Optimization (Spark 3.0+)
# Join on range conditions
events.join(ranges, 
    (events.timestamp >= ranges.start_time) & 
    (events.timestamp <= ranges.end_time))

# 4. Multiple Join Optimization
# ❌ BAD: Multiple sequential joins
df1.join(df2, "key").join(df3, "key").join(df4, "key")

# ✅ GOOD: Filter before join
df1_filtered = df1.filter(col("date") >= "2024-01-01")
df2_filtered = df2.filter(col("status") == "active")
df1_filtered.join(df2_filtered, "key")

# 5. Self-join optimization
# Find duplicate records
from pyspark.sql.functions import md5, concat_ws

df.withColumn("hash", md5(concat_ws("||", *df.columns)))
  .groupBy("hash")
  .count()
  .filter(col("count") > 1)
```

---

## 3. Spark Streaming & Real-time Processing

### Q15: Structured Streaming Concepts and Windowing
**Answer:**

```python
from pyspark.sql.functions import window, col, avg, count

# 1. Read from Kafka
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "events") \
    .load()

# 2. Parse JSON data
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

schema = StructType([
    StructField("user_id", StringType()),
    StructField("event_type", StringType()),
    StructField("value", DoubleType()),
    StructField("timestamp", TimestampType())
])

events_df = kafka_df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

# 3. Tumbling Window (non-overlapping)
windowed_df = events_df \
    .groupBy(
        window(col("timestamp"), "10 minutes"),
        col("event_type")
    ) \
    .agg(
        count("*").alias("event_count"),
        avg("value").alias("avg_value")
    )

# 4. Sliding Window (overlapping)
sliding_window_df = events_df \
    .groupBy(
        window(col("timestamp"), "10 minutes", "5 minutes"),  # 10 min window, slide every 5 min
        col("event_type")
    ) \
    .agg(count("*").alias("event_count"))

# 5. Watermark for late data handling
watermarked_df = events_df \
    .withWatermark("timestamp", "10 minutes") \
    .groupBy(
        window(col("timestamp"), "5 minutes"),
        col("event_type")
    ) \
    .count()

# 6. Write to output
query = watermarked_df.writeStream \
    .format("parquet") \
    .option("path", "/output/path") \
    .option("checkpointLocation", "/checkpoint/path") \
    .outputMode("append") \
    .trigger(processingTime="1 minute") \
    .start()

query.awaitTermination()
```

### Q16: Handling State in Streaming (mapGroupsWithState)
**Answer:**

```python
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType
from pyspark.sql import Row

# Define state schema
class SessionState:
    def __init__(self, count=0, start_time=0, last_time=0):
        self.count = count
        self.start_time = start_time
        self.last_time = last_time

def update_session_state(key, values, state):
    """
    key: user_id
    values: iterator of events
    state: GroupState object
    """
    # Get existing state or initialize
    if state.exists:
        session = state.get
    else:
        session = SessionState()
    
    # Process new events
    for value in values:
        session.count += 1
        if session.start_time == 0:
            session.start_time = value.timestamp
        session.last_time = value.timestamp
    
    # Check for timeout (session gap > 30 minutes)
    timeout_duration = 30 * 60 * 1000  # 30 minutes in ms
    if session.last_time - session.start_time > timeout_duration:
        # Session ended, remove state
        state.remove()
        return (key, session.count, session.start_time, session.last_time)
    else:
        # Update state
        state.update(session)
        return (key, session.count, session.start_time, session.last_time)

# Apply stateful transformation
result = events_df.groupByKey(lambda x: x.user_id) \
    .mapGroupsWithState(update_session_state, GroupStateTimeout.EventTimeTimeout())

# Write output
result.writeStream \
    .format("console") \
    .outputMode("update") \
    .start()
```

---

# 💾 SQL - Advanced Interview Questions

## 1. Complex SQL Queries

### Q17: Write a query to find second highest salary by department
**Answer:**

```sql
-- Method 1: Using DENSE_RANK()
SELECT dept_id, emp_name, salary
FROM (
    SELECT 
        dept_id,
        emp_name,
        salary,
        DENSE_RANK() OVER (PARTITION BY dept_id ORDER BY salary DESC) as rank
    FROM employees
) ranked
WHERE rank = 2;

-- Method 2: Using subquery
SELECT dept_id, MAX(salary) as second_highest
FROM employees
WHERE salary < (
    SELECT MAX(salary)
    FROM employees e2
    WHERE e2.dept_id = employees.dept_id
)
GROUP BY dept_id;

-- Method 3: Using OFFSET
SELECT DISTINCT dept_id, salary
FROM (
    SELECT 
        dept_id,
        salary,
        ROW_NUMBER() OVER (PARTITION BY dept_id ORDER BY salary DESC) as rn
    FROM employees
) t
WHERE rn = 2;
```

### Q18: Find employees earning more than their manager
**Answer:**

```sql
-- Self join
SELECT e1.emp_name, e1.salary as emp_salary, 
       e2.emp_name as manager_name, e2.salary as manager_salary
FROM employees e1
JOIN employees e2 ON e1.manager_id = e2.emp_id
WHERE e1.salary > e2.salary;

-- Using CTE
WITH employee_manager AS (
    SELECT 
        e.emp_id,
        e.emp_name,
        e.salary as emp_salary,
        m.salary as manager_salary
    FROM employees e
    LEFT JOIN employees m ON e.manager_id = m.emp_id
)
SELECT * FROM employee_manager
WHERE emp_salary > manager_salary;
```

### Q19: Find running total and moving average
**Answer:**

```sql
-- Running total and 3-day moving average
SELECT 
    date,
    sales,
    -- Running total
    SUM(sales) OVER (ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as running_total,
    
    -- Moving average (3 days)
    AVG(sales) OVER (ORDER BY date ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) as moving_avg_3day,
    
    -- Previous day sales
    LAG(sales, 1) OVER (ORDER BY date) as prev_day_sales,
    
    -- % change from previous day
    ROUND(
        ((sales - LAG(sales, 1) OVER (ORDER BY date)) / 
        LAG(sales, 1) OVER (ORDER BY date)) * 100, 2
    ) as pct_change
FROM daily_sales
ORDER BY date;
```

### Q20: Complex JOIN - Find customers who bought product A but not product B
**Answer:**

```sql
-- Method 1: Using NOT EXISTS
SELECT DISTINCT c.customer_id, c.customer_name
FROM customers c
JOIN orders o ON c.customer_id = o.customer_id
WHERE o.product_id = 'A'
AND NOT EXISTS (
    SELECT 1
    FROM orders o2
    WHERE o2.customer_id = c.customer_id
    AND o2.product_id = 'B'
);

-- Method 2: Using LEFT JOIN and IS NULL
SELECT DISTINCT c.customer_id, c.customer_name
FROM customers c
JOIN orders o1 ON c.customer_id = o1.customer_id AND o1.product_id = 'A'
LEFT JOIN orders o2 ON c.customer_id = o2.customer_id AND o2.product_id = 'B'
WHERE o2.order_id IS NULL;

-- Method 3: Using set operations
SELECT customer_id, customer_name
FROM customers
WHERE customer_id IN (
    SELECT customer_id FROM orders WHERE product_id = 'A'
    EXCEPT
    SELECT customer_id FROM orders WHERE product_id = 'B'
);
```

### Q21: Find gaps in sequential data
**Answer:**

```sql
-- Find missing IDs in sequence
WITH RECURSIVE all_ids AS (
    SELECT 1 as id
    UNION ALL
    SELECT id + 1
    FROM all_ids
    WHERE id < (SELECT MAX(id) FROM orders)
)
SELECT a.id as missing_id
FROM all_ids a
LEFT JOIN orders o ON a.id = o.id
WHERE o.id IS NULL;

-- Find date gaps
SELECT 
    prev_date,
    next_date,
    DATEDIFF(next_date, prev_date) - 1 as gap_days
FROM (
    SELECT 
        order_date as prev_date,
        LEAD(order_date) OVER (ORDER BY order_date) as next_date
    FROM orders
) t
WHERE DATEDIFF(next_date, prev_date) > 1;
```

### Q22: Hierarchical Query - Employee Reporting Structure
**Answer:**

```sql
-- Recursive CTE for organizational hierarchy
WITH RECURSIVE employee_hierarchy AS (
    -- Anchor: Top level managers
    SELECT 
        emp_id,
        emp_name,
        manager_id,
        0 as level,
        CAST(emp_name AS VARCHAR(1000)) as path
    FROM employees
    WHERE manager_id IS NULL
    
    UNION ALL
    
    -- Recursive: Employees under managers
    SELECT 
        e.emp_id,
        e.emp_name,
        e.manager_id,
        eh.level + 1,
        CONCAT(eh.path, ' -> ', e.emp_name) as path
    FROM employees e
    JOIN employee_hierarchy eh ON e.manager_id = eh.emp_id
)
SELECT 
    REPEAT('  ', level) || emp_name as organization_chart,
    level,
    path
FROM employee_hierarchy
ORDER BY path;

-- Find all subordinates of a manager (including indirect)
WITH RECURSIVE subordinates AS (
    SELECT emp_id, emp_name, manager_id
    FROM employees
    WHERE emp_id = 100  -- Manager ID
    
    UNION ALL
    
    SELECT e.emp_id, e.emp_name, e.manager_id
    FROM employees e
    JOIN subordinates s ON e.manager_id = s.emp_id
)
SELECT * FROM subordinates;
```

### Q23: Complex Aggregation - Pivot and Unpivot
**Answer:**

```sql
-- PIVOT: Convert rows to columns
SELECT 
    product,
    SUM(CASE WHEN quarter = 'Q1' THEN sales ELSE 0 END) as Q1,
    SUM(CASE WHEN quarter = 'Q2' THEN sales ELSE 0 END) as Q2,
    SUM(CASE WHEN quarter = 'Q3' THEN sales ELSE 0 END) as Q3,
    SUM(CASE WHEN quarter = 'Q4' THEN sales ELSE 0 END) as Q4
FROM sales_data
GROUP BY product;

-- UNPIVOT: Convert columns to rows
SELECT product, 'Q1' as quarter, Q1 as sales FROM quarterly_sales
UNION ALL
SELECT product, 'Q2' as quarter, Q2 as sales FROM quarterly_sales
UNION ALL
SELECT product, 'Q3' as quarter, Q3 as sales FROM quarterly_sales
UNION ALL
SELECT product, 'Q4' as quarter, Q4 as sales FROM quarterly_sales;

-- Dynamic PIVOT using CASE expressions
SELECT 
    category,
    SUM(sales) as total_sales,
    SUM(CASE WHEN YEAR(date) = 2023 THEN sales ELSE 0 END) as sales_2023,
    SUM(CASE WHEN YEAR(date) = 2024 THEN sales ELSE 0 END) as sales_2024,
    ROUND(
        (SUM(CASE WHEN YEAR(date) = 2024 THEN sales ELSE 0 END) -
         SUM(CASE WHEN YEAR(date) = 2023 THEN sales ELSE 0 END)) /
        NULLIF(SUM(CASE WHEN YEAR(date) = 2023 THEN sales ELSE 0 END), 0) * 100,
        2
    ) as yoy_growth
FROM sales
GROUP BY category;
```

### Q24: Advanced Date/Time Queries
**Answer:**

```sql
-- 1. First and last day of month
SELECT 
    DATE_TRUNC('month', current_date) as first_day_of_month,
    LAST_DAY(current_date) as last_day_of_month,
    DATE_TRUNC('year', current_date) as first_day_of_year;

-- 2. Business days between two dates (excluding weekends)
SELECT 
    start_date,
    end_date,
    DATEDIFF(end_date, start_date) as total_days,
    DATEDIFF(end_date, start_date) - 
    (2 * ((DATEDIFF(end_date, start_date) / 7))) -
    (CASE WHEN DAYOFWEEK(start_date) = 1 THEN 1 ELSE 0 END) -
    (CASE WHEN DAYOFWEEK(end_date) = 7 THEN 1 ELSE 0 END) as business_days
FROM date_ranges;

-- 3. Find records from last N days/months
SELECT *
FROM orders
WHERE order_date >= CURRENT_DATE - INTERVAL 30 DAY;  -- Last 30 days

SELECT *
FROM orders
WHERE order_date >= DATE_TRUNC('month', CURRENT_DATE - INTERVAL 3 MONTH);  -- Last 3 months

-- 4. Time bucket aggregation
SELECT 
    DATE_TRUNC('hour', timestamp) as hour,
    COUNT(*) as event_count,
    AVG(value) as avg_value
FROM events
WHERE timestamp >= CURRENT_TIMESTAMP - INTERVAL 1 DAY
GROUP BY DATE_TRUNC('hour', timestamp)
ORDER BY hour;

-- 5. Session calculation (gap > 30 minutes = new session)
WITH session_starts AS (
    SELECT 
        user_id,
        event_time,
        CASE 
            WHEN LAG(event_time) OVER (PARTITION BY user_id ORDER BY event_time) IS NULL
                OR TIMESTAMPDIFF(MINUTE, 
                     LAG(event_time) OVER (PARTITION BY user_id ORDER BY event_time),
                     event_time) > 30
            THEN 1 ELSE 0
        END as is_new_session
    FROM user_events
),
sessions AS (
    SELECT 
        user_id,
        event_time,
        SUM(is_new_session) OVER (PARTITION BY user_id ORDER BY event_time) as session_id
    FROM session_starts
)
SELECT 
    user_id,
    session_id,
    MIN(event_time) as session_start,
    MAX(event_time) as session_end,
    COUNT(*) as events_in_session,
    TIMESTAMPDIFF(MINUTE, MIN(event_time), MAX(event_time)) as session_duration_minutes
FROM sessions
GROUP BY user_id, session_id;
```

### Q25: Performance Optimization Queries
**Answer:**

```sql
-- 1. Identify slow queries (using query history)
SELECT 
    query_text,
    total_elapsed_time / 1000 as execution_time_sec,
    rows_produced,
    bytes_scanned / (1024*1024*1024) as gb_scanned,
    warehouse_size,
    start_time
FROM information_schema.query_history
WHERE execution_time_sec > 60
ORDER BY execution_time_sec DESC
LIMIT 100;

-- 2. Find missing indexes (table scan operations)
-- Check execution plan for table scans
EXPLAIN 
SELECT * FROM large_table WHERE non_indexed_column = 'value';

-- 3. Optimize with covering indexes
CREATE INDEX idx_covering ON orders (customer_id, order_date, status);

-- Query uses only indexed columns (index-only scan)
SELECT customer_id, order_date, status
FROM orders
WHERE customer_id = 123;

-- 4. Partition pruning check
SELECT *
FROM partitioned_table
WHERE partition_column = '2024-01-01';  -- Uses partition pruning

-- 5. Avoid correlated subqueries
-- ❌ BAD: Correlated subquery (runs for each row)
SELECT emp_name, salary
FROM employees e1
WHERE salary > (
    SELECT AVG(salary)
    FROM employees e2
    WHERE e2.dept_id = e1.dept_id
);

-- ✅ GOOD: Join with pre-computed aggregates
WITH dept_avg AS (
    SELECT dept_id, AVG(salary) as avg_salary
    FROM employees
    GROUP BY dept_id
)
SELECT e.emp_name, e.salary
FROM employees e
JOIN dept_avg d ON e.dept_id = d.dept_id
WHERE e.salary > d.avg_salary;
```

---

# ☁️ Databricks Specific Questions

### Q26: Explain Delta Lake ACID properties
**Answer:**

Delta Lake provides ACID transactions on data lakes:

```python
# 1. ATOMICITY - All or nothing writes
df.write.format("delta").mode("append").save("/path/to/delta_table")

# 2. CONSISTENCY - Schema enforcement
df_with_wrong_schema.write.format("delta").mode("append").save("/path")  # Will fail

# 3. ISOLATION - Read isolation with Time Travel
# Read version from 1 hour ago while writes are happening
df_historical = spark.read.format("delta").option("timestampAsOf", "2024-01-01 10:00:00").load("/path")

# 4. DURABILITY - Transaction log ensures durability
# Check transaction log
display(spark.sql("DESCRIBE HISTORY delta.`/path/to/table`"))

# Schema Evolution
df.write.format("delta") \
    .option("mergeSchema", "true") \
    .mode("append") \
    .save("/path")

# Time Travel
# By version
df_v5 = spark.read.format("delta").option("versionAsOf", 5).load("/path")

# By timestamp
df_yesterday = spark.read.format("delta") \
    .option("timestampAsOf", "2024-01-15") \
    .load("/path")

# Restore to previous version
spark.sql("RESTORE TABLE my_table TO VERSION AS OF 5")
```

### Q27: Delta Lake Optimization - OPTIMIZE and Z-ORDER
**Answer:**

```python
# 1. OPTIMIZE - Compaction (combines small files)
spark.sql("OPTIMIZE delta.`/path/to/table`")

# 2. Z-ORDERING - Data clustering for better pruning
spark.sql("""
    OPTIMIZE delta.`/path/to/table`
    ZORDER BY (customer_id, order_date)
""")

# 3. VACUUM - Delete old files (after 7 days default retention)
spark.sql("VACUUM delta.`/path/to/table` RETAIN 168 HOURS")  # 7 days

# 4. Check file statistics
display(spark.sql("DESCRIBE DETAIL delta.`/path/to/table`"))

# 5. Automatic optimization (Databricks)
spark.sql("""
    ALTER TABLE my_table 
    SET TBLPROPERTIES (
        'delta.autoOptimize.optimizeWrite' = 'true',
        'delta.autoOptimize.autoCompact' = 'true'
    )
""")

# 6. Data skipping statistics
# Delta automatically collects min/max stats for first 32 columns
# Query with predicate will skip files based on stats
df = spark.read.format("delta").load("/path")
df.filter(col("order_date") == "2024-01-01").count()  # Skips irrelevant files
```

### Q28: Delta Lake MERGE (UPSERT) Operations
**Answer:**

```python
from delta.tables import DeltaTable

# Setup
target = DeltaTable.forPath(spark, "/path/to/target")
updates = spark.read.parquet("/path/to/updates")

# Basic MERGE (UPSERT)
target.alias("target").merge(
    updates.alias("updates"),
    "target.id = updates.id"
).whenMatchedUpdateAll() \
 .whenNotMatchedInsertAll() \
 .execute()

# Advanced MERGE with conditions
target.alias("t").merge(
    updates.alias("u"),
    "t.customer_id = u.customer_id AND t.order_id = u.order_id"
).whenMatchedUpdate(
    condition = "u.last_updated > t.last_updated",  # Only update if newer
    set = {
        "status": "u.status",
        "amount": "u.amount",
        "last_updated": "u.last_updated"
    }
).whenNotMatchedInsert(
    values = {
        "customer_id": "u.customer_id",
        "order_id": "u.order_id",
        "status": "u.status",
        "amount": "u.amount",
        "created_date": "current_timestamp()",
        "last_updated": "u.last_updated"
    }
).whenNotMatchedBySource().update(
    condition = "t.status = 'ACTIVE'",
    set = {"status": "lit('INACTIVE')"}
).execute()

# SCD Type 2 Implementation
target.alias("target").merge(
    updates.alias("updates"),
    """target.business_key = updates.business_key 
       AND target.is_current = true"""
).whenMatchedUpdate(
    condition = "target.value != updates.value",
    set = {
        "is_current": "false",
        "end_date": "current_date()"
    }
).whenNotMatchedInsert(
    values = {
        "business_key": "updates.business_key",
        "value": "updates.value",
        "is_current": "true",
        "start_date": "current_date()",
        "end_date": "null"
    }
).execute()

# Insert new version of changed records
new_versions = spark.sql("""
    SELECT 
        u.business_key,
        u.value,
        true as is_current,
        current_date() as start_date,
        cast(null as date) as end_date
    FROM updates u
    INNER JOIN target t 
        ON u.business_key = t.business_key 
        AND t.is_current = true
        AND t.value != u.value
""")

new_versions.write.format("delta").mode("append").save("/path/to/target")
```

### Q29: Databricks Auto Loader (Incremental File Processing)
**Answer:**

```python
# Auto Loader for incremental ingestion
checkpoint_path = "/checkpoint/orders"
schema_location = "/schema/orders"

# Infer schema and process new files automatically
df = spark.readStream.format("cloudFiles") \
    .option("cloudFiles.format", "json") \
    .option("cloudFiles.schemaLocation", schema_location) \
    .option("cloudFiles.inferColumnTypes", "true") \
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns") \
    .load("/landing/orders/")

# Process and write to Delta
query = df.writeStream \
    .format("delta") \
    .option("checkpointLocation", checkpoint_path) \
    .option("mergeSchema", "true") \
    .trigger(availableNow=True) \
    .table("bronze.orders")

# Wait for batch to complete
query.awaitTermination()

# With transformations
transformed_df = df \
    .withColumn("ingestion_time", current_timestamp()) \
    .withColumn("year", year(col("order_date"))) \
    .withColumn("month", month(col("order_date")))

query = transformed_df.writeStream \
    .format("delta") \
    .partitionBy("year", "month") \
    .option("checkpointLocation", checkpoint_path) \
    .trigger(availableNow=True) \
    .table("silver.orders_processed")
```

### Q30: Databricks Workflows and Job Optimization
**Answer:**

```python
# 1. Configure cluster for optimal performance
# Databricks cluster configuration (JSON)
{
  "cluster_name": "production-etl",
  "spark_version": "13.3.x-scala2.12",
  "node_type_id": "i3.2xlarge",
  "num_workers": 10,
  "autoscale": {
    "min_workers": 2,
    "max_workers": 20
  },
  "spark_conf": {
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
    "spark.databricks.delta.optimizeWrite.enabled": "true",
    "spark.databricks.delta.autoCompact.enabled": "true",
    "spark.sql.adaptive.skewJoin.enabled": "true",
    "spark.sql.files.maxPartitionBytes": "128MB"
  },
  "enable_elastic_disk": true,
  "photon_enabled": true  # For Delta Lake workloads
}

# 2. Job orchestration with dependencies
# jobs_config.json
{
  "name": "Daily ETL Pipeline",
  "tasks": [
    {
      "task_key": "bronze_ingestion",
      "notebook_task": {
        "notebook_path": "/ETL/01_bronze_layer"
      }
    },
    {
      "task_key": "silver_transformation",
      "depends_on": [{"task_key": "bronze_ingestion"}],
      "notebook_task": {
        "notebook_path": "/ETL/02_silver_layer"
      }
    },
    {
      "task_key": "gold_aggregation",
      "depends_on": [{"task_key": "silver_transformation"}],
      "notebook_task": {
        "notebook_path": "/ETL/03_gold_layer"
      }
    }
  ],
  "schedule": {
    "quartz_cron_expression": "0 0 2 * * ?",
    "timezone_id": "America/New_York"
  }
}

# 3. Photon engine optimization
# Enable Photon for vectorized execution
spark.conf.set("spark.databricks.photon.enabled", "true")

# Photon works best with:
# - Parquet and Delta formats
# - Simple data types (avoid UDFs)
# - Native Spark operations

# 4. Monitor job performance
# Get cluster metrics
metrics = spark.conf.get("spark.databricks.clusterUsageTags.clusterAllTags")

# Query job history
job_runs = spark.sql("""
    SELECT 
        job_id,
        run_id,
        start_time,
        end_time,
        result_state,
        (end_time - start_time) / 1000 as duration_seconds
    FROM system.workflow.job_runs
    WHERE job_id = 'your_job_id'
    ORDER BY start_time DESC
    LIMIT 10
""")
```

---

# 🚀 PySpark Optimization & Performance

### Q31: Memory Management Best Practices
**Answer:**

```python
# 1. Monitor memory usage
# Check executor memory
spark.sparkContext._conf.get('spark.executor.memory')

# Get current DataFrame size in memory
df.cache()
print(f"Size in memory: {df.storageLevel}")

# 2. Broadcast variables for small lookup data
broadcast_var = spark.sparkContext.broadcast(lookup_dict)
df.rdd.map(lambda x: broadcast_var.value.get(x.key))

# 3. Accumulator for distributed counting
counter = spark.sparkContext.accumulator(0)

def process_row(row):
    global counter
    counter += 1
    return row

df.rdd.foreach(process_row)
print(f"Processed rows: {counter.value}")

# 4. Avoid collecting large datasets
# ❌ BAD
large_df.collect()  # OOM error

# ✅ GOOD
large_df.write.parquet("output/")  # Write directly
large_df.limit(100).collect()  # Sample data

# 5. Use appropriate storage levels
from pyspark import StorageLevel

# For iterative algorithms
df.persist(StorageLevel.MEMORY_AND_DISK)

# For one-time use
df.unpersist()  # Free memory

# 6. Monitor GC (Garbage Collection)
# Add to spark config:
# spark.executor.extraJavaOptions=-XX:+UseG1GC -XX:+PrintGCDetails
```

### Q32: Partition Tuning for Optimal Performance
**Answer:**

```python
# 1. Calculate optimal partitions
def calculate_optimal_partitions(data_size_gb, partition_size_mb=128):
    """
    data_size_gb: Size of data in GB
    partition_size_mb: Target partition size in MB (default 128MB)
    """
    return int((data_size_gb * 1024) / partition_size_mb)

# Example: 100GB data
optimal_parts = calculate_optimal_partitions(100)  # ~800 partitions
spark.conf.set("spark.sql.shuffle.partitions", optimal_parts)

# 2. Check current partitions
print(f"Current partitions: {df.rdd.getNumPartitions()}")

# 3. Repartition strategies
# Before wide transformations (shuffle operations)
df_repart = df.repartition(200, "partition_key")

# After filtering (reduce partitions)
df_filtered = df.filter(col("status") == "active")  # May have empty partitions
df_coalesced = df_filtered.coalesce(50)  # Reduce partitions without shuffle

# 4. Partition by column for better performance
df.write.partitionBy("year", "month").parquet("output/")

# 5. Check partition distribution (detect skew)
df.groupBy(spark_partition_id()).count().show()

# 6. Adaptive partition coalescing (Spark 3.0+)
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.minPartitionNum", "1")
spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128MB")
```

### Q33: Catalyst Optimizer - Understand Query Plans
**Answer:**

```python
# 1. Explain query plans
df = spark.read.parquet("large_table")
filtered_df = df.filter(col("year") == 2024) \
                .select("id", "name", "amount") \
                .groupBy("name").sum("amount")

# Physical plan
filtered_df.explain()

# Extended explanation
filtered_df.explain(extended=True)

# Formatted (Spark 3.0+)
filtered_df.explain(mode="formatted")

# Cost-based
filtered_df.explain(mode="cost")

# 2. Check for optimizations
query_plan = filtered_df._jdf.queryExecution().executedPlan()

# Example output interpretation:
"""
== Physical Plan ==
AdaptiveSparkPlan (6)
+- HashAggregate (5)
   +- Exchange (4)              # <-- Shuffle operation
      +- HashAggregate (3)
         +- Project (2)          # <-- Column pruning applied
            +- Filter (1)        # <-- Predicate pushdown applied
               +- FileScan      # <-- Read only required columns
"""

# 3. Check if predicate pushdown works
df = spark.read.parquet("partitioned_table")
df.filter(col("date") == "2024-01-01").explain()
# Look for "PushedFilters" in output

# 4. Broadcast join detection
small_df = spark.read.parquet("dimension")
large_df = spark.read.parquet("fact")

result = large_df.join(small_df, "key")
result.explain()
# Look for "BroadcastHashJoin" or "BroadcastExchange"

# 5. Check statistics for CBO (Cost-Based Optimization)
spark.sql("ANALYZE TABLE my_table COMPUTE STATISTICS")
spark.sql("ANALYZE TABLE my_table COMPUTE STATISTICS FOR COLUMNS col1, col2")

# View statistics
spark.sql("DESCRIBE EXTENDED my_table").show(truncate=False)
```

### Q34: Handling Large Datasets - Best Practices
**Answer:**

```python
# 1. Incremental processing with watermark
from pyspark.sql.functions import window

# Process only last 7 days
last_week = datetime.now() - timedelta(days=7)
df = spark.read.parquet("events/") \
    .filter(col("event_date") >= last_week)

# 2. Sampling for development/testing
# 10% sample
sample_df = df.sample(fraction=0.1, seed=42)

# Stratified sampling
stratified_sample = df.sampleBy("category", fractions={'A': 0.1, 'B': 0.2, 'C': 0.05})

# 3. Lazy evaluation + caching strategy
# Cache intermediate results used multiple times
intermediate = df.filter(col("status") == "active").cache()

result1 = intermediate.groupBy("category").count()
result2 = intermediate.groupBy("region").sum("amount")

intermediate.unpersist()  # Clean up

# 4. Avoid expensive operations
# ❌ BAD: collect() on large dataset
all_data = large_df.collect()

# ✅ GOOD: Process in partitions
large_df.foreachPartition(lambda partition: process_partition(partition))

# 5. Use columnar formats
# Parquet: Good for read-heavy workloads, compression
df.write.parquet("output/")

# ORC: Good for Hive integration
df.write.orc("output/")

# Delta: ACID, Time Travel, Schema Evolution
df.write.format("delta").save("output/")

# 6. Compression
# Snappy: Fast, moderate compression (default)
spark.conf.set("spark.sql.parquet.compression.codec", "snappy")

# Gzip: Slower, better compression
spark.conf.set("spark.sql.parquet.compression.codec", "gzip")

# 7. File size optimization
# Target 128MB - 1GB per file
df.repartition(100).write.parquet("output/")

# Or use maxRecordsPerFile
df.write.option("maxRecordsPerFile", 1000000).parquet("output/")
```

### Q35: Debugging and Monitoring Spark Jobs
**Answer:**

```python
# 1. Spark UI - Key metrics to monitor
"""
Stages Tab:
- Duration: Long-running stages
- Tasks: Failed tasks
- Shuffle Read/Write: High shuffle indicates optimization needed
- GC Time: High GC time indicates memory pressure

Storage Tab:
- Check cached RDDs/DataFrames
- Memory usage

Executors Tab:
- Task failures
- GC time per executor
- Memory usage

SQL Tab:
- Query plans
- Duration breakdown
"""

# 2. Add custom logging
import logging

logger = logging.getLogger(__name__)

def process_data():
    logger.info("Starting data processing")
    
    start_time = time.time()
    df = spark.read.parquet("input/")
    logger.info(f"Read completed in {time.time() - start_time:.2f}s")
    
    logger.info(f"Input records: {df.count()}")
    
    # Process...
    result = df.filter(col("amount") > 0)
    
    logger.info(f"Output records: {result.count()}")
    logger.info(f"Filter selectivity: {result.count() / df.count():.2%}")
    
    return result

# 3. Track task metrics
from pyspark import TaskContext

def track_partition_size(partition):
    ctx = TaskContext.get()
    if ctx:
        print(f"Partition ID: {ctx.partitionId()}")
        print(f"Attempt number: {ctx.attemptNumber()}")
    
    count = 0
    for row in partition:
        count += 1
        yield row
    
    print(f"Partition {ctx.partitionId()} size: {count}")

df.rdd.mapPartitions(track_partition_size).count()

# 4. Custom metrics using accumulator
from pyspark.accumulators import AccumulatorParam

class SetAccumulatorParam(AccumulatorParam):
    def zero(self, value):
        return set()
    
    def addInPlace(self, val1, val2):
        return val1.union(val2)

unique_values = spark.sparkContext.accumulator(set(), SetAccumulatorParam())

def collect_uniques(row):
    global unique_values
    unique_values.add(row.category)
    return row

df.rdd.foreach(collect_uniques)
print(f"Unique categories: {unique_values.value}")

# 5. Performance profiling
# Enable event logging
spark.conf.set("spark.eventLog.enabled", "true")
spark.conf.set("spark.eventLog.dir", "/tmp/spark-events")

# Use Spark's built-in profiler
sc.setJobDescription("ETL Job - Bronze to Silver")
# Your code here
sc.setJobDescription("Next stage")

# 6. Monitor in real-time
# Get active Spark session metrics
active_streams = spark.streams.active
print(f"Active streaming queries: {len(active_streams)}")

for stream in active_streams:
    print(f"ID: {stream.id}, Status: {stream.status}")
```

---

# 🌍 Real-World Scenarios

### Q36: Design a Real-Time Fraud Detection System
**Answer:**

```python
from pyspark.sql.functions import window, col, when, count, sum, avg, stddev
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

# 1. Stream transactions from Kafka
transaction_schema = StructType([
    StructField("transaction_id", StringType()),
    StructField("user_id", StringType()),
    StructField("amount", DoubleType()),
    StructField("merchant_id", StringType()),
    StructField("location", StringType()),
    StructField("timestamp", TimestampType())
])

transactions = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "transactions") \
    .load() \
    .select(from_json(col("value").cast("string"), transaction_schema).alias("data")) \
    .select("data.*")

# 2. Feature engineering for fraud detection
features = transactions \
    .withWatermark("timestamp", "10 minutes") \
    .withColumn("hour", hour("timestamp")) \
    .withColumn("day_of_week", dayofweek("timestamp"))

# 3. Calculate user statistics (5-minute window)
user_stats = features \
    .groupBy(
        window("timestamp", "5 minutes", "1 minute"),
        "user_id"
    ) \
    .agg(
        count("*").alias("txn_count_5m"),
        sum("amount").alias("total_amount_5m"),
        avg("amount").alias("avg_amount_5m"),
        stddev("amount").alias("stddev_amount_5m"),
        countDistinct("merchant_id").alias("distinct_merchants_5m")
    )

# 4. Fraud rules
fraud_detected = features.join(user_stats, ["user_id"]) \
    .withColumn("is_fraud",
        when(
            (col("txn_count_5m") > 10) |  # More than 10 txns in 5 min
            (col("total_amount_5m") > 10000) |  # Total amount > $10k
            (col("amount") > col("avg_amount_5m") + 3 * col("stddev_amount_5m")) |  # 3 std devs
            (col("distinct_merchants_5m") > 5)  # Multiple merchants
        , lit(1))
        .otherwise(lit(0))
    )

# 5. Write fraud alerts to Kafka and store in Delta
fraud_alerts = fraud_detected.filter(col("is_fraud") == 1)

# To Kafka for real-time alerting
fraud_alerts.selectExpr("transaction_id as key", "to_json(struct(*)) as value") \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "fraud_alerts") \
    .option("checkpointLocation", "/checkpoint/fraud_alerts") \
    .start()

# To Delta for analytics
fraud_alerts.writeStream \
    .format("delta") \
    .option("checkpointLocation", "/checkpoint/fraud_history") \
    .outputMode("append") \
    .table("fraud.detected_transactions")

# 6. ML-based fraud detection (batch scoring)
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.feature import VectorAssembler

# Load pre-trained model
rf_model = RandomForestClassifier.load("/models/fraud_detection")

# Real-time scoring
ml_features = VectorAssembler(
    inputCols=["amount", "hour", "day_of_week", "txn_count_5m", "avg_amount_5m"],
    outputCol="features"
).transform(features)

predictions = rf_model.transform(ml_features)

predictions.writeStream \
    .format("delta") \
    .option("checkpointLocation", "/checkpoint/ml_predictions") \
    .table("fraud.ml_predictions")
```

### Q37: Build a Lambda Architecture Data Pipeline
**Answer:**

```python
# Lambda Architecture: Batch Layer + Speed Layer + Serving Layer

# === BATCH LAYER (Historical Data) ===
def batch_layer():
    """Process historical data (T-1 and older)"""
    
    # Read from data lake
    raw_events = spark.read.parquet("s3://datalake/raw/events/")
    
    # Complex transformations
    batch_aggregates = raw_events \
        .filter(col("event_date") < current_date()) \
        .groupBy("user_id", "event_type", "event_date") \
        .agg(
            count("*").alias("event_count"),
            sum("value").alias("total_value"),
            collect_list("properties").alias("all_properties")
        )
    
    # Write to serving layer (Delta Lake)
    batch_aggregates.write \
        .format("delta") \
        .mode("overwrite") \
        .partitionBy("event_date") \
        .option("replaceWhere", "event_date < current_date()") \
        .save("s3://serving/batch_views/user_events")
    
    # Optimize for fast queries
    spark.sql("OPTIMIZE delta.`s3://serving/batch_views/user_events` ZORDER BY (user_id)")

# === SPEED LAYER (Real-time Stream) ===
def speed_layer():
    """Process real-time streaming data"""
    
    # Read from Kafka
    stream_events = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "events") \
        .option("startingOffsets", "latest") \
        .load() \
        .select(from_json(col("value").cast("string"), event_schema).alias("data")) \
        .select("data.*")
    
    # Real-time aggregations with watermark
    realtime_aggregates = stream_events \
        .withWatermark("event_timestamp", "10 minutes") \
        .groupBy(
            window("event_timestamp", "1 minute"),
            "user_id",
            "event_type"
        ) \
        .agg(
            count("*").alias("event_count"),
            sum("value").alias("total_value")
        )
    
    # Write to speed layer table
    query = realtime_aggregates.writeStream \
        .format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", "/checkpoint/speed_layer") \
        .table("speed_views.user_events_realtime")
    
    return query

# === SERVING LAYER (Query Interface) ===
def serve_user_metrics(user_id, start_date, end_date):
    """Combine batch and speed layers for queries"""
    
    # Batch view (historical data)
    batch_metrics = spark.read.format("delta") \
        .load("s3://serving/batch_views/user_events") \
        .filter(
            (col("user_id") == user_id) &
            (col("event_date") >= start_date) &
            (col("event_date") < current_date())
        )
    
    # Speed view (today's data)
    speed_metrics = spark.read.format("delta") \
        .table("speed_views.user_events_realtime") \
        .filter(
            (col("user_id") == user_id) &
            (col("window.start") >= current_date())
        ) \
        .select(
            col("user_id"),
            date(col("window.start")).alias("event_date"),
            col("event_type"),
            col("event_count"),
            col("total_value")
        )
    
    # Merge batch and speed layer results
    combined_metrics = batch_metrics.unionAll(speed_metrics) \
        .groupBy("user_id", "event_type") \
        .agg(
            sum("event_count").alias("total_events"),
            sum("total_value").alias("total_value")
        )
    
    return combined_metrics

# === ORCHESTRATION ===
# Run batch layer daily
batch_layer()  # Scheduled via Databricks Jobs/Airflow

# Run speed layer continuously
speed_query = speed_layer()
speed_query.awaitTermination()

# Query serving layer
user_metrics = serve_user_metrics("user_123", "2024-01-01", "2024-01-31")
user_metrics.show()
```

### Q38: Implement SCD Type 2 (Slowly Changing Dimension)
**Answer:**

```python
from delta.tables import DeltaTable
from pyspark.sql.functions import lit, current_timestamp, col, when

def implement_scd_type2(source_df, target_path, business_keys, compare_columns):
    """
    Implement SCD Type 2 pattern
    
    Args:
        source_df: New/updated records
        target_path: Path to Delta table
        business_keys: List of columns for matching (e.g., ['customer_id'])
        compare_columns: Columns to check for changes
    """
    
    # Check if target exists
    try:
        target = DeltaTable.forPath(spark, target_path)
    except:
        # Initialize target with SCD columns
        source_df \
            .withColumn("is_current", lit(True)) \
            .withColumn("effective_date", current_timestamp()) \
            .withColumn("end_date", lit(None).cast("timestamp")) \
            .withColumn("version", lit(1)) \
            .write.format("delta").save(target_path)
        return
    
    # Build merge condition
    merge_condition = " AND ".join([f"target.{key} = source.{key}" for key in business_keys])
    merge_condition += " AND target.is_current = true"
    
    # Build change detection condition
    change_condition = " OR ".join([
        f"target.{col} != source.{col}" for col in compare_columns
    ])
    
    # Prepare source with metadata
    source_prepared = source_df \
        .withColumn("is_current", lit(True)) \
        .withColumn("effective_date", current_timestamp()) \
        .withColumn("end_date", lit(None).cast("timestamp")) \
        .withColumn("version", lit(1))
    
    # Step 1: Close current records that have changes
    target.alias("target").merge(
        source_prepared.alias("source"),
        merge_condition
    ).whenMatchedUpdate(
        condition = change_condition,
        set = {
            "is_current": "false",
            "end_date": "current_timestamp()"
        }
    ).execute()
    
    # Step 2: Insert new versions of changed records
    changed_records = source_prepared.alias("source") \
        .join(
            target.toDF().alias("target"),
            on=business_keys,
            how="inner"
        ) \
        .where(change_condition) \
        .select("source.*") \
        .withColumn("version", col("version") + 1)
    
    changed_records.write.format("delta").mode("append").save(target_path)
    
    # Step 3: Insert completely new records
    existing_keys = target.toDF().select(*business_keys).distinct()
    
    new_records = source_prepared.alias("source") \
        .join(existing_keys.alias("target"), on=business_keys, how="left_anti")
    
    new_records.write.format("delta").mode("append").save(target_path)

# Example usage
# Current customer data
current_customers = spark.table("bronze.customers_snapshot")

# Implement SCD Type 2
implement_scd_type2(
    source_df=current_customers,
    target_path="/gold/customers_scd",
    business_keys=["customer_id"],
    compare_columns=["name", "email", "address", "phone"]
)

# Query historical data
scd_table = spark.read.format("delta").load("/gold/customers_scd")

# Get current records
current_records = scd_table.filter(col("is_current") == True)

# Get historical records for a customer
customer_history = scd_table \
    .filter(col("customer_id") == "CUST_123") \
    .orderBy("effective_date")

# Point-in-time query (as of specific date)
as_of_date = "2024-01-15"
historical_snapshot = scd_table \
    .filter(
        (col("effective_date") <= as_of_date) &
        ((col("end_date") > as_of_date) | col("end_date").isNull())
    )
```

---

# 🏗️ System Design for Data Engineering

### Q39: Design a Data Lake Architecture (Bronze-Silver-Gold)
**Answer:**

```python
"""
MEDALLION ARCHITECTURE (Bronze -> Silver -> Gold)

Bronze Layer: Raw data (immutable)
- Keep data in original format
- No transformations, just ingestion
- Schema-on-read

Silver Layer: Cleaned & enriched data
- Data quality checks
- Deduplication
- Schema enforcement
- Joins with dimension tables

Gold Layer: Business-level aggregates
- Aggregated metrics
- Curated datasets for analytics
- Optimized for queries
"""

# === BRONZE LAYER ===
def ingest_to_bronze(source_path, bronze_path, source_format="json"):
    """Raw data ingestion with Auto Loader"""
    
    df = spark.readStream \
        .format("cloudFiles") \
        .option("cloudFiles.format", source_format) \
        .option("cloudFiles.schemaLocation", f"{bronze_path}/_schema") \
        .option("cloudFiles.inferColumnTypes", "true") \
        .load(source_path) \
        .withColumn("_ingestion_timestamp", current_timestamp()) \
        .withColumn("_source_file", input_file_name())
    
    query = df.writeStream \
        .format("delta") \
        .option("checkpointLocation", f"{bronze_path}/_checkpoint") \
        .option("mergeSchema", "true") \
        .trigger(availableNow=True) \
        .start(bronze_path)
    
    return query

# === SILVER LAYER ===
def bronze_to_silver(bronze_path, silver_path):
    """Clean, deduplicate, and enrich data"""
    
    # Read bronze
    bronze_df = spark.readStream \
        .format("delta") \
        .load(bronze_path)
    
    # Data quality rules
    from pyspark.sql.functions import col, when, regexp_replace, trim, lower
    
    silver_df = bronze_df \
        .dropDuplicates(["id", "timestamp"]) \
        .filter(col("id").isNotNull()) \
        .filter(col("amount") > 0) \
        .withColumn("email", lower(trim(col("email")))) \
        .withColumn("phone", regexp_replace(col("phone"), "[^0-9]", "")) \
        .withColumn("_quality_check_timestamp", current_timestamp()) \
        .withColumn("_is_valid", 
            when(
                (col("email").rlike("^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$")) &
                (col("amount") > 0) &
                (col("phone").isNotNull()),
                lit(True)
            ).otherwise(lit(False))
        )
    
    # Write valid records to silver
    valid_records = silver_df.filter(col("_is_valid") == True)
    
    query = valid_records.writeStream \
        .format("delta") \
        .option("checkpointLocation", f"{silver_path}/_checkpoint") \
        .option("mergeSchema", "true") \
        .trigger(availableNow=True) \
        .start(silver_path)
    
    # Write invalid records to quarantine
    invalid_records = silver_df.filter(col("_is_valid") == False)
    
    invalid_query = invalid_records.writeStream \
        .format("delta") \
        .option("checkpointLocation", f"{silver_path}/_quarantine_checkpoint") \
        .trigger(availableNow=True) \
        .start(f"{silver_path}_quarantine")
    
    return query, invalid_query

# === GOLD LAYER ===
def silver_to_gold(silver_path, gold_path):
    """Create business-level aggregates"""
    
    silver_df = spark.read.format("delta").load(silver_path)
    
    # Daily aggregates
    daily_metrics = silver_df \
        .groupBy(
            date_trunc("day", col("timestamp")).alias("date"),
            "customer_id",
            "product_category"
        ) \
        .agg(
            count("*").alias("transaction_count"),
            sum("amount").alias("total_amount"),
            avg("amount").alias("avg_amount"),
            min("amount").alias("min_amount"),
            max("amount").alias("max_amount"),
            countDistinct("product_id").alias("unique_products")
        ) \
        .withColumn("_aggregated_at", current_timestamp())
    
    # Write to gold with partitioning
    daily_metrics.write \
        .format("delta") \
        .mode("overwrite") \
        .partitionBy("date") \
        .option("replaceWhere", f"date >= '{start_date}'") \
        .save(gold_path)
    
    # Optimize for queries
    spark.sql(f"""
        OPTIMIZE delta.`{gold_path}`
        ZORDER BY (customer_id, product_category)
    """)
    
    return daily_metrics

# === ORCHESTRATION ===
# Bronze ingestion
bronze_query = ingest_to_bronze(
    source_path="s3://raw-data/transactions/",
    bronze_path="/mnt/bronze/transactions"
)

# Silver transformation
silver_query, quarantine_query = bronze_to_silver(
    bronze_path="/mnt/bronze/transactions",
    silver_path="/mnt/silver/transactions"
)

# Gold aggregation
gold_metrics = silver_to_gold(
    silver_path="/mnt/silver/transactions",
    gold_path="/mnt/gold/daily_metrics"
)
```

### Q40: Design a Streaming ETL Pipeline with Exactly-Once Semantics
**Answer:**

```python
"""
Exactly-Once Processing Requirements:
1. Idempotent writes
2. Checkpointing
3. Transaction support (Delta Lake)
4. Duplicate detection
"""

from pyspark.sql.functions import col, expr, window, current_timestamp, sha2, concat_ws

def create_exactly_once_pipeline():
    
    # 1. Read from Kafka with checkpointing
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "transactions") \
        .option("startingOffsets", "earliest") \
        .option("maxOffsetsPerTrigger", 10000) \
        .option("failOnDataLoss", "false") \
        .load()
    
    # 2. Parse and add deduplication key
    from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
    
    schema = StructType([
        StructField("transaction_id", StringType()),
        StructField("user_id", StringType()),
        StructField("amount", DoubleType()),
        StructField("timestamp", TimestampType())
    ])
    
    parsed_df = kafka_df \
        .select(
            col("key").cast("string"),
            from_json(col("value").cast("string"), schema).alias("data"),
            col("topic"),
            col("partition"),
            col("offset"),
            col("timestamp").alias("kafka_timestamp")
        ) \
        .select("data.*", "topic", "partition", "offset", "kafka_timestamp") \
        .withColumn("dedup_key", 
            sha2(concat_ws("||", col("transaction_id"), col("timestamp")), 256)
        )
    
    # 3. Stateful deduplication using dropDuplicates
    deduplicated_df = parsed_df \
        .withWatermark("timestamp", "10 minutes") \
        .dropDuplicates(["dedup_key"])
    
    # 4. Apply business logic
    processed_df = deduplicated_df \
        .withColumn("processing_timestamp", current_timestamp()) \
        .withColumn("amount_category",
            when(col("amount") < 100, "small")
            .when(col("amount") < 1000, "medium")
            .otherwise("large")
        )
    
    # 5. Write with exactly-once semantics to Delta Lake
    # Delta Lake ensures ACID transactions
    
    def write_to_delta(batch_df, batch_id):
        """
        Custom write logic for exactly-once semantics
        batch_id ensures idempotency
        """
        from delta.tables import DeltaTable
        
        target_path = "/data/processed/transactions"
        
        # Check if target exists
        if DeltaTable.isDeltaTable(spark, target_path):
            target = DeltaTable.forPath(spark, target_path)
            
            # MERGE for upsert (handles duplicates)
            target.alias("target").merge(
                batch_df.alias("source"),
                "target.transaction_id = source.transaction_id"
            ).whenMatchedUpdateAll() \
             .whenNotMatchedInsertAll() \
             .execute()
        else:
            # First write
            batch_df.write.format("delta").save(target_path)
        
        # Log batch metadata for monitoring
        batch_metadata = spark.createDataFrame([{
            "batch_id": batch_id,
            "record_count": batch_df.count(),
            "processed_at": datetime.now()
        }])
        
        batch_metadata.write.format("delta").mode("append") \
            .save("/data/metadata/batch_log")
    
    # 6. Start streaming query with checkpointing
    query = processed_df.writeStream \
        .foreachBatch(write_to_delta) \
        .option("checkpointLocation", "/checkpoints/exactly_once_pipeline") \
        .trigger(processingTime="30 seconds") \
        .start()
    
    return query

# Additional: Monitor streaming query
def monitor_stream(query):
    while query.isActive:
        status = query.status
        print(f"Message processing rate: {status['processingRate']}")
        print(f"Input rate: {status['inputRate']}")
        print(f"Batch duration: {status['batchDuration']}")
        
        # Check for lag
        progress = query.lastProgress
        if progress:
            input_rows = progress['numInputRows']
            processed_rows = progress['processedRowsPerSecond']
            print(f"Input rows: {input_rows}, Processed: {processed_rows}")
        
        time.sleep(30)

# Run pipeline
query = create_exactly_once_pipeline()
monitor_stream(query)
```

---

# 📚 Practice Projects

## Project 1: Build a Real-Time E-commerce Analytics Platform
**Technologies:** Spark Streaming, Kafka, Delta Lake, Databricks

**Requirements:**
1. Ingest clickstream data from Kafka
2. Real-time user session tracking
3. Product recommendation engine
4. Real-time dashboard metrics (sales, inventory)
5. Fraud detection alerts

## Project 2: Data Lake Migration
**Technologies:** S3, Parquet, Delta Lake, Glue/Databricks

**Requirements:**
1. Migrate from relational database to data lake
2. Implement incremental ingestion
3. Build Bronze-Silver-Gold layers
4. Optimize for query performance
5. Implement data quality checks

## Project 3: Log Analytics Platform
**Technologies:** Spark, ElasticSearch, Kibana

**Requirements:**
1. Parse and process application logs
2. Real-time anomaly detection
3. Log aggregation and search
4. Performance monitoring dashboard
5. Alert system for critical errors

---

# 🎓 Study Roadmap (4-6 Weeks)

## Week 1-2: Spark Fundamentals & Advanced Concepts
- [ ] Review Spark architecture (Driver, Executors, DAG)
- [ ] Practice transformations and actions
- [ ] Master window functions
- [ ] Understand Catalyst optimizer
- [ ] Work with different join types
- [ ] Practice with complex nested data

## Week 3: SQL Mastery
- [ ] Practice 50+ complex SQL queries
- [ ] Window functions (LAG, LEAD, RANK, ROW_NUMBER)
- [ ] CTEs and recursive queries
- [ ] Query optimization techniques
- [ ] Execution plan analysis

## Week 4: Databricks & Delta Lake
- [ ] Delta Lake ACID operations
- [ ] Time Travel and versioning
- [ ] OPTIMIZE and Z-ORDER
- [ ] Auto Loader for incremental ingestion
- [ ] Databricks jobs and workflows

## Week 5-6: Real-World Projects & System Design
- [ ] Build end-to-end data pipelines
- [ ] Implement streaming applications
- [ ] Practice system design questions
- [ ] Performance tuning and optimization
- [ ] Mock interviews

---

# 💡 Interview Tips

1. **Explain with Examples**: Always provide code examples when explaining concepts

2. **Discuss Trade-offs**: Mention pros/cons of different approaches

3. **Production Experience**: Share real-world challenges you've faced

4. **Optimization**: Show you think about performance, scalability

5. **Best Practices**: Mention testing, monitoring, error handling

6. **Business Context**: Connect technical decisions to business value

---

# 📖 Additional Resources

**Official Documentation:**
- Apache Spark: https://spark.apache.org/docs/latest/
- Databricks: https://docs.databricks.com/
- Delta Lake: https://docs.delta.io/

**Practice Platforms:**
- LeetCode (SQL): https://leetcode.com/problemset/database/
- HackerRank (SQL): https://www.hackerrank.com/domains/sql
- DataCamp: https://www.datacamp.com/

**Books:**
- "Learning Spark, 2nd Edition" by Jules S. Damji et al.
- "Spark: The Definitive Guide" by Bill Chambers & Matei Zaharia
- "High Performance Spark" by Holden Karau

---

## 🚀 Good Luck with Your Interviews!

Remember: Focus on understanding concepts deeply rather than memorizing answers. 
Practice coding daily, and always think about real-world applications!

