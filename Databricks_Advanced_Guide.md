# ⚡ Databricks Advanced Interview Guide
## Delta Lake, Unity Catalog, Photon, & Production Best Practices

---

## 📋 Table of Contents
1. [Delta Lake Deep Dive](#delta-lake-deep-dive)
2. [Unity Catalog](#unity-catalog)
3. [Databricks SQL & Warehouses](#databricks-sql)
4. [Photon Engine](#photon-engine)
5. [MLflow Integration](#mlflow-integration)
6. [Production Workflows](#production-workflows)
7. [Cost Optimization](#cost-optimization)
8. [Security & Governance](#security-governance)

---

# Delta Lake Deep Dive

## Transaction Log & ACID Properties

### Q1: Explain Delta Lake Transaction Log (_delta_log)
**Answer:**

```python
# Transaction log structure
"""
/delta_table/
  /_delta_log/
    /00000000000000000000.json  # Initial commit
    /00000000000000000001.json  # Second commit
    /00000000000000000002.json  # Third commit
    /00000000000000000010.checkpoint.parquet  # Checkpoint at commit 10
"""

# Read transaction log
log_df = spark.read.json("/path/to/table/_delta_log/00000000000000000000.json")
log_df.printSchema()

# View table history
history_df = spark.sql("DESCRIBE HISTORY delta.`/path/to/table`")
display(history_df)

# Transaction log contains:
# 1. Add files: New data files added
# 2. Remove files: Files marked for deletion
# 3. Metadata: Schema, partition columns
# 4. Protocol: Delta Lake version
# 5. CommitInfo: User, timestamp, operation

# Read specific version
df_v5 = spark.read.format("delta").option("versionAsOf", 5).load("/path/to/table")

# Read by timestamp
df_yesterday = spark.read.format("delta") \
    .option("timestampAsOf", "2024-01-15 10:00:00") \
    .load("/path/to/table")
```

### Q2: Time Travel and Version Management
**Answer:**

```python
from delta.tables import DeltaTable

# Get table history
delta_table = DeltaTable.forPath(spark, "/path/to/table")
history = delta_table.history()
display(history.select("version", "timestamp", "operation", "operationMetrics"))

# Query old versions
df_v3 = spark.read.format("delta").option("versionAsOf", 3).load("/path/to/table")
df_7_days_ago = spark.read.format("delta") \
    .option("timestampAsOf", "2024-01-08") \
    .load("/path/to/table")

# Restore table to previous version
# Restore by version
spark.sql("RESTORE TABLE my_table TO VERSION AS OF 5")

# Restore by timestamp
spark.sql("RESTORE TABLE my_table TO TIMESTAMP AS OF '2024-01-15 10:00:00'")

# Clone table (deep vs shallow)
# Shallow clone (metadata only, fast)
spark.sql("""
    CREATE TABLE IF NOT EXISTS dev.my_table_clone
    SHALLOW CLONE prod.my_table
    VERSION AS OF 10
""")

# Deep clone (copies data, slower)
spark.sql("""
    CREATE TABLE IF NOT EXISTS backup.my_table_backup
    DEEP CLONE prod.my_table
""")

# Get version at specific timestamp
from datetime import datetime, timedelta

restore_time = datetime.now() - timedelta(hours=2)
df = spark.read.format("delta") \
    .option("timestampAsOf", restore_time.strftime("%Y-%m-%d %H:%M:%S")) \
    .load("/path/to/table")
```

### Q3: VACUUM and Data Retention
**Answer:**

```python
# VACUUM removes old files not needed for time travel
# Default retention: 7 days

# Basic vacuum (7 day retention)
spark.sql("VACUUM delta.`/path/to/table`")

# Custom retention period
spark.sql("VACUUM delta.`/path/to/table` RETAIN 168 HOURS")  # 7 days

# Dry run to see what would be deleted
spark.sql("VACUUM delta.`/path/to/table` DRY RUN")

# Set retention at table level
spark.sql("""
    ALTER TABLE my_table 
    SET TBLPROPERTIES (
        'delta.logRetentionDuration' = 'interval 30 days',
        'delta.deletedFileRetentionDuration' = 'interval 7 days'
    )
""")

# ⚠️ Warning: Short retention for development only
spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
spark.sql("VACUUM delta.`/path/to/table` RETAIN 0 HOURS")
spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "true")

# Check table size before and after vacuum
def get_table_size(path):
    files = dbutils.fs.ls(path)
    total_size = sum([f.size for f in files if not f.name.startswith("_delta_log")])
    return total_size / (1024**3)  # GB

print(f"Before VACUUM: {get_table_size('/path/to/table'):.2f} GB")
spark.sql("VACUUM delta.`/path/to/table` RETAIN 168 HOURS")
print(f"After VACUUM: {get_table_size('/path/to/table'):.2f} GB")
```

### Q4: OPTIMIZE and Z-ORDER
**Answer:**

```python
# OPTIMIZE: Combines small files into larger ones
# Reduces file count, improves query performance

# Basic optimize
spark.sql("OPTIMIZE delta.`/path/to/table`")

# Optimize specific partition
spark.sql("""
    OPTIMIZE delta.`/path/to/table`
    WHERE date >= '2024-01-01'
""")

# Z-ORDER: Co-locate related data for better pruning
# Use for columns frequently used in WHERE clauses
spark.sql("""
    OPTIMIZE delta.`/path/to/table`
    ZORDER BY (customer_id, product_id, order_date)
""")

# Check optimization results
spark.sql("""
    DESCRIBE HISTORY delta.`/path/to/table`
""").filter("operation = 'OPTIMIZE'").show()

# Auto-optimize configuration
spark.sql("""
    ALTER TABLE my_table SET TBLPROPERTIES (
        'delta.autoOptimize.optimizeWrite' = 'true',
        'delta.autoOptimize.autoCompact' = 'true'
    )
""")

# Check file statistics
detail_df = spark.sql("DESCRIBE DETAIL delta.`/path/to/table`")
display(detail_df.select("numFiles", "sizeInBytes", "properties"))

# Optimize workflow
def optimize_table(table_path, zorder_columns=None, partition_filter=None):
    """
    Optimize Delta table with optional Z-ORDER and partition filter
    """
    print(f"Starting optimization of {table_path}")
    
    # Get initial stats
    initial_stats = spark.sql(f"DESCRIBE DETAIL delta.`{table_path}`").collect()[0]
    print(f"Initial: {initial_stats['numFiles']} files, {initial_stats['sizeInBytes']/1e9:.2f} GB")
    
    # Build optimize command
    optimize_cmd = f"OPTIMIZE delta.`{table_path}`"
    
    if partition_filter:
        optimize_cmd += f" WHERE {partition_filter}"
    
    if zorder_columns:
        zorder_cols = ", ".join(zorder_columns)
        optimize_cmd += f" ZORDER BY ({zorder_cols})"
    
    # Execute optimize
    spark.sql(optimize_cmd)
    
    # Get final stats
    final_stats = spark.sql(f"DESCRIBE DETAIL delta.`{table_path}`").collect()[0]
    print(f"Final: {final_stats['numFiles']} files, {final_stats['sizeInBytes']/1e9:.2f} GB")
    print(f"Reduced files by: {initial_stats['numFiles'] - final_stats['numFiles']}")

# Usage
optimize_table(
    "/path/to/table",
    zorder_columns=["customer_id", "order_date"],
    partition_filter="date >= '2024-01-01'"
)
```

### Q5: MERGE Operations (UPSERT, SCD Type 2)
**Answer:**

```python
from delta.tables import DeltaTable
from pyspark.sql.functions import current_timestamp, col, lit

# Basic MERGE (Upsert)
target = DeltaTable.forPath(spark, "/path/to/target")
updates = spark.read.parquet("/path/to/updates")

target.alias("t").merge(
    updates.alias("u"),
    "t.id = u.id"
).whenMatchedUpdateAll() \
 .whenNotMatchedInsertAll() \
 .execute()

# Conditional UPDATE (only if changed)
target.alias("t").merge(
    updates.alias("u"),
    "t.id = u.id"
).whenMatchedUpdate(
    condition = "t.updated_at < u.updated_at",
    set = {
        "value": "u.value",
        "updated_at": "u.updated_at"
    }
).whenNotMatchedInsert(
    values = {
        "id": "u.id",
        "value": "u.value",
        "created_at": "current_timestamp()",
        "updated_at": "u.updated_at"
    }
).execute()

# SCD Type 2 Implementation
def scd_type2_merge(target_path, source_df, business_key, compare_columns):
    """
    Implement Slowly Changing Dimension Type 2
    """
    target = DeltaTable.forPath(spark, target_path)
    
    # Build comparison condition
    change_condition = " OR ".join([
        f"t.{col} <> s.{col}" for col in compare_columns
    ])
    
    # Stage 1: Mark old records as inactive
    target.alias("t").merge(
        source_df.alias("s"),
        f"t.{business_key} = s.{business_key} AND t.is_current = true"
    ).whenMatchedUpdate(
        condition = change_condition,
        set = {
            "is_current": "false",
            "end_date": "current_timestamp()"
        }
    ).execute()
    
    # Stage 2: Insert new records
    new_records = source_df.alias("s") \
        .join(
            target.toDF().filter("is_current = true").alias("t"),
            col(f"s.{business_key}") == col(f"t.{business_key}"),
            "left"
        ) \
        .where(" OR ".join([
            f"t.{col} IS NULL" for col in compare_columns
        ]) + f" OR {change_condition}") \
        .select("s.*") \
        .withColumn("is_current", lit(True)) \
        .withColumn("start_date", current_timestamp()) \
        .withColumn("end_date", lit(None).cast("timestamp"))
    
    new_records.write.format("delta").mode("append").save(target_path)

# DELETE with conditions
target = DeltaTable.forPath(spark, "/path/to/table")
target.delete("status = 'INACTIVE' AND last_active < '2023-01-01'")

# Update with join
target.alias("t").merge(
    lookup_df.alias("l"),
    "t.category_id = l.category_id"
).whenMatchedUpdate(
    set = {"category_name": "l.category_name"}
).execute()
```

### Q6: Schema Evolution and Enforcement
**Answer:**

```python
# Enable schema evolution
df = spark.read.parquet("/new/data/with/extra/columns")

# Option 1: mergeSchema for new columns
df.write.format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \
    .save("/path/to/table")

# Option 2: overwriteSchema to replace schema
df.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save("/path/to/table")

# Schema enforcement (default behavior)
# Will fail if schema doesn't match
try:
    wrong_schema_df.write.format("delta").mode("append").save("/path/to/table")
except Exception as e:
    print(f"Schema mismatch: {e}")

# Alter table schema
spark.sql("""
    ALTER TABLE my_table 
    ADD COLUMNS (new_column STRING, another_column INT)
""")

# Change column type (limited support)
spark.sql("""
    ALTER TABLE my_table 
    ALTER COLUMN column_name TYPE STRING
""")

# Add column with default value
spark.sql("""
    ALTER TABLE my_table 
    ADD COLUMN status STRING DEFAULT 'active'
""")

# Drop column (Delta Lake 2.3+)
spark.sql("ALTER TABLE my_table DROP COLUMN old_column")

# Rename column
spark.sql("ALTER TABLE my_table RENAME COLUMN old_name TO new_name")

# Get table schema
schema = spark.table("my_table").schema
print(schema.simpleString())

# Compare schemas
def compare_schemas(path1, path2):
    schema1 = spark.read.format("delta").load(path1).schema
    schema2 = spark.read.format("delta").load(path2).schema
    
    set1 = set([(f.name, str(f.dataType)) for f in schema1.fields])
    set2 = set([(f.name, str(f.dataType)) for f in schema2.fields])
    
    print("Only in schema1:", set1 - set2)
    print("Only in schema2:", set2 - set1)
    print("Common:", set1 & set2)
```

---

# Unity Catalog

### Q7: Unity Catalog Three-Level Namespace
**Answer:**

```python
"""
Unity Catalog Hierarchy:
Metastore (top level)
  └── Catalog (database of databases)
      └── Schema (database)
          └── Tables/Views/Functions
          
Example: catalog_name.schema_name.table_name
"""

# Create catalog
spark.sql("CREATE CATALOG IF NOT EXISTS production")

# Create schema
spark.sql("CREATE SCHEMA IF NOT EXISTS production.sales")

# Create table with full path
spark.sql("""
    CREATE TABLE production.sales.orders (
        order_id BIGINT,
        customer_id BIGINT,
        order_date DATE,
        amount DECIMAL(10,2)
    )
    USING DELTA
    PARTITIONED BY (order_date)
""")

# Set default catalog
spark.sql("USE CATALOG production")

# Set default schema
spark.sql("USE SCHEMA sales")

# Now can reference table without full path
df = spark.table("orders")

# Grant permissions
spark.sql("""
    GRANT SELECT, MODIFY ON TABLE production.sales.orders 
    TO `user@company.com`
""")

# Create external location
spark.sql("""
    CREATE EXTERNAL LOCATION my_s3_location
    URL 's3://my-bucket/data/'
    WITH (STORAGE CREDENTIAL aws_credential)
""")

# Register external table
spark.sql("""
    CREATE TABLE production.sales.external_data
    USING DELTA
    LOCATION 's3://my-bucket/data/table/'
""")

# List catalogs
display(spark.sql("SHOW CATALOGS"))

# List schemas
display(spark.sql("SHOW SCHEMAS IN production"))

# List tables
display(spark.sql("SHOW TABLES IN production.sales"))

# Get table details
display(spark.sql("DESCRIBE EXTENDED production.sales.orders"))
```

### Q8: Data Governance and Access Control
**Answer:**

```python
# Row-level security (using functions)
spark.sql("""
    CREATE FUNCTION production.sales.get_user_region()
    RETURNS STRING
    RETURN current_user()  -- Simplified example
""")

# Create view with row filtering
spark.sql("""
    CREATE VIEW production.sales.orders_filtered AS
    SELECT * FROM production.sales.orders
    WHERE region = production.sales.get_user_region()
""")

# Column-level security
spark.sql("""
    GRANT SELECT (order_id, customer_id, amount) 
    ON TABLE production.sales.orders 
    TO `analyst_group`
""")

# Dynamic views for masking
spark.sql("""
    CREATE VIEW production.sales.customers_masked AS
    SELECT 
        customer_id,
        CASE 
            WHEN is_member('admin_group') THEN email
            ELSE CONCAT(LEFT(email, 3), '***@***.com')
        END as email,
        CASE 
            WHEN is_member('finance_group') THEN credit_card
            ELSE 'XXXX-XXXX-XXXX-' || RIGHT(credit_card, 4)
        END as credit_card
    FROM production.sales.customers
""")

# Audit logging
spark.sql("""
    SELECT 
        event_time,
        user_identity,
        action_name,
        request_params.full_name_arg as table_name,
        response.status_code
    FROM system.access.audit
    WHERE action_name IN ('createTable', 'getTable', 'deleteTable')
    AND event_date >= current_date() - 7
    ORDER BY event_time DESC
""")

# Data lineage
spark.sql("""
    SELECT 
        entity_type,
        entity_id,
        upstream_entities,
        downstream_entities
    FROM system.access.table_lineage
    WHERE entity_id = 'production.sales.orders'
""")

# Grant usage on catalog
spark.sql("GRANT USAGE ON CATALOG production TO `data_team`")
spark.sql("GRANT USAGE ON SCHEMA production.sales TO `data_team`")
spark.sql("GRANT SELECT ON TABLE production.sales.orders TO `data_team`")

# Create service principal
spark.sql("""
    CREATE SERVICE PRINCIPAL 'etl-pipeline'
""")

spark.sql("""
    GRANT MODIFY ON SCHEMA production.bronze TO SERVICE PRINCIPAL `etl-pipeline`
""")
```

### Q9: Data Sharing (Delta Sharing)
**Answer:**

```python
# Enable Delta Sharing on table
spark.sql("""
    ALTER TABLE production.sales.orders
    SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
""")

# Create share
spark.sql("""
    CREATE SHARE sales_share
    COMMENT 'Share sales data with partners'
""")

# Add table to share
spark.sql("""
    ALTER SHARE sales_share
    ADD TABLE production.sales.orders
""")

# Create recipient
spark.sql("""
    CREATE RECIPIENT partner_company
    USING ID '<recipient-identifier>'
""")

# Grant access
spark.sql("""
    GRANT SELECT ON SHARE sales_share TO RECIPIENT partner_company
""")

# View shares
display(spark.sql("SHOW SHARES"))

# View share permissions
display(spark.sql("SHOW GRANT ON SHARE sales_share"))

# Read shared data (as recipient)
shared_df = spark.read.format("deltaSharing") \
    .option("share", "sales_share") \
    .option("table", "orders") \
    .load()

# Change Data Feed (CDC)
# Enable CDC
spark.sql("""
    ALTER TABLE production.sales.orders
    SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
""")

# Read changes
changes_df = spark.read.format("delta") \
    .option("readChangeFeed", "true") \
    .option("startingVersion", 10) \
    .option("endingVersion", 20) \
    .table("production.sales.orders")

display(changes_df.select("_change_type", "_commit_version", "_commit_timestamp", "*"))

# CDC with streaming
cdc_stream = spark.readStream \
    .format("delta") \
    .option("readChangeFeed", "true") \
    .option("startingVersion", 10) \
    .table("production.sales.orders")

cdc_stream.writeStream \
    .format("delta") \
    .option("checkpointLocation", "/checkpoint/cdc") \
    .table("production.sales.orders_cdc")
```

---

# Databricks SQL & Warehouses

### Q10: SQL Warehouse Configuration
**Answer:**

```python
# SQL Warehouse types:
# 1. Serverless: Fast startup, auto-scaling
# 2. Pro: Standard performance
# 3. Classic: Legacy

# Create SQL warehouse via API
import requests

workspace_url = "https://<workspace>.databricks.com"
token = dbutils.secrets.get("scope", "token")

warehouse_config = {
    "name": "Analytics Warehouse",
    "cluster_size": "Medium",  # X-Small, Small, Medium, Large, X-Large
    "min_num_clusters": 1,
    "max_num_clusters": 10,
    "auto_stop_mins": 10,
    "tags": {
        "Environment": "Production",
        "Team": "Analytics"
    },
    "spot_instance_policy": "COST_OPTIMIZED",  # RELIABILITY_OPTIMIZED, COST_OPTIMIZED
    "enable_photon": True,
    "enable_serverless_compute": True,
    "warehouse_type": "PRO"
}

response = requests.post(
    f"{workspace_url}/api/2.0/sql/warehouses",
    headers={"Authorization": f"Bearer {token}"},
    json=warehouse_config
)

# Query result caching
spark.sql("""
    SELECT /*+ CACHE */ 
        customer_id,
        COUNT(*) as order_count,
        SUM(amount) as total_amount
    FROM production.sales.orders
    WHERE order_date >= '2024-01-01'
    GROUP BY customer_id
""")

# Materialized views (coming soon)
# Currently use Delta Live Tables or scheduled jobs
```

### Q11: Query Performance Optimization
**Answer:**

```python
# Use partition pruning
spark.sql("""
    SELECT * FROM production.sales.orders
    WHERE order_date = '2024-01-15'  -- Uses partition pruning
""")

# Data skipping (Delta statistics)
spark.sql("""
    SELECT * FROM production.sales.orders
    WHERE customer_id = 12345  -- Skips files based on min/max stats
""")

# Broadcast join hint
spark.sql("""
    SELECT /*+ BROADCAST(dim) */ 
        fact.order_id,
        dim.product_name
    FROM production.sales.orders fact
    JOIN production.sales.products dim ON fact.product_id = dim.product_id
""")

# Result caching
spark.sql("""
    CACHE SELECT customer_id, SUM(amount) as total
    FROM production.sales.orders
    GROUP BY customer_id
""")

# Check query execution plan
spark.sql("EXPLAIN EXTENDED SELECT * FROM production.sales.orders WHERE order_date = '2024-01-15'").show(truncate=False)

# Use Delta cache (instance local cache)
spark.conf.set("spark.databricks.io.cache.enabled", "true")
spark.conf.set("spark.databricks.io.cache.maxDiskUsage", "50g")
spark.conf.set("spark.databricks.io.cache.maxMetaDataCache", "1g")

# Query profiling
query_id = spark.sql("""
    SELECT 
        customer_id,
        COUNT(*) as order_count,
        SUM(amount) as revenue
    FROM production.sales.orders
    WHERE order_date >= '2024-01-01'
    GROUP BY customer_id
""")._jdf.queryExecution().executedPlan().toString()

print(query_id)
```

---

# Photon Engine

### Q12: Photon Optimization
**Answer:**

```python
# Photon is vectorized query engine for Delta Lake
# Automatic in SQL Warehouses, opt-in for clusters

# Enable Photon on cluster
# Set in cluster configuration:
# Runtime: "Photon Runtime"
# Or via API:
cluster_config = {
    "runtime_engine": "PHOTON",
    "spark_version": "13.3.x-photon-scala2.12"
}

# Photon works best with:
# 1. Delta Lake tables
# 2. Parquet files
# 3. SQL operations (not RDD)
# 4. Native data types (not UDFs)

# ✅ GOOD: Photon accelerated
df = spark.table("production.sales.orders") \
    .filter(col("order_date") >= "2024-01-01") \
    .groupBy("customer_id") \
    .agg(
        sum("amount").alias("total_amount"),
        count("*").alias("order_count")
    )

# ❌ BAD: Photon cannot optimize
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

@udf(returnType=StringType())
def custom_transform(value):
    return value.upper()

df_with_udf = df.withColumn("transformed", custom_transform(col("customer_id")))

# Photon performance monitoring
spark.sql("""
    SELECT 
        query_id,
        query_text,
        execution_duration,
        photon_execution_time_ms,
        photon_execution_time_ms * 100.0 / execution_duration as photon_percentage
    FROM system.query.history
    WHERE photon_execution_time_ms > 0
    ORDER BY execution_duration DESC
    LIMIT 100
""")

# Photon vs Standard comparison
def benchmark_query(query, use_photon=True):
    import time
    
    if use_photon:
        spark.conf.set("spark.databricks.photon.enabled", "true")
    else:
        spark.conf.set("spark.databricks.photon.enabled", "false")
    
    start = time.time()
    result = spark.sql(query)
    result.count()  # Force execution
    duration = time.time() - start
    
    return duration

query = """
    SELECT 
        customer_id,
        product_category,
        COUNT(*) as order_count,
        SUM(amount) as total_amount
    FROM production.sales.orders
    WHERE order_date >= '2024-01-01'
    GROUP BY customer_id, product_category
"""

standard_time = benchmark_query(query, use_photon=False)
photon_time = benchmark_query(query, use_photon=True)

print(f"Standard: {standard_time:.2f}s")
print(f"Photon: {photon_time:.2f}s")
print(f"Speedup: {standard_time / photon_time:.2f}x")
```

---

# Production Workflows

### Q13: Databricks Workflows (Jobs)
**Answer:**

```python
# Job configuration (JSON)
job_config = {
    "name": "Daily ETL Pipeline",
    "tags": {
        "environment": "production",
        "team": "data-engineering"
    },
    "tasks": [
        {
            "task_key": "bronze_ingestion",
            "description": "Ingest raw data to bronze layer",
            "notebook_task": {
                "notebook_path": "/ETL/bronze/ingest_orders",
                "base_parameters": {
                    "date": "{{job.start_time.date}}"
                }
            },
            "new_cluster": {
                "spark_version": "13.3.x-scala2.12",
                "node_type_id": "i3.xlarge",
                "num_workers": 5,
                "spark_conf": {
                    "spark.databricks.delta.optimizeWrite.enabled": "true"
                }
            },
            "libraries": [
                {"pypi": {"package": "pandas==2.0.0"}}
            ],
            "timeout_seconds": 3600,
            "max_retries": 2,
            "retry_on_timeout": True
        },
        {
            "task_key": "silver_transformation",
            "depends_on": [{"task_key": "bronze_ingestion"}],
            "notebook_task": {
                "notebook_path": "/ETL/silver/transform_orders"
            },
            "existing_cluster_id": "{{cluster_id}}"
        },
        {
            "task_key": "gold_aggregation",
            "depends_on": [{"task_key": "silver_transformation"}],
            "spark_python_task": {
                "python_file": "dbfs:/scripts/aggregate_sales.py",
                "parameters": ["--date", "{{job.start_time.date}}"]
            }
        },
        {
            "task_key": "data_quality_check",
            "depends_on": [{"task_key": "gold_aggregation"}],
            "sql_task": {
                "warehouse_id": "{{warehouse_id}}",
                "query": {
                    "query_id": "{{quality_check_query_id}}"
                }
            }
        },
        {
            "task_key": "send_notification",
            "depends_on": [{"task_key": "data_quality_check"}],
            "notebook_task": {
                "notebook_path": "/Utilities/send_email_notification"
            }
        }
    ],
    "schedule": {
        "quartz_cron_expression": "0 0 2 * * ?",  # Daily at 2 AM
        "timezone_id": "America/New_York",
        "pause_status": "UNPAUSED"
    },
    "max_concurrent_runs": 1,
    "email_notifications": {
        "on_success": ["team@company.com"],
        "on_failure": ["oncall@company.com"],
        "no_alert_for_skipped_runs": False
    },
    "notification_settings": {
        "no_alert_for_skipped_runs": False,
        "no_alert_for_canceled_runs": False
    },
    "timeout_seconds": 14400,  # 4 hours
    "git_source": {
        "git_url": "https://github.com/company/data-pipelines",
        "git_provider": "gitHub",
        "git_branch": "main"
    }
}

# Create job via API
import requests

response = requests.post(
    f"{workspace_url}/api/2.1/jobs/create",
    headers={"Authorization": f"Bearer {token}"},
    json=job_config
)

job_id = response.json()['job_id']
print(f"Created job: {job_id}")

# Run job
run_response = requests.post(
    f"{workspace_url}/api/2.1/jobs/run-now",
    headers={"Authorization": f"Bearer {token}"},
    json={"job_id": job_id}
)

run_id = run_response.json()['run_id']

# Monitor job run
status_response = requests.get(
    f"{workspace_url}/api/2.1/jobs/runs/get",
    headers={"Authorization": f"Bearer {token}"},
    params={"run_id": run_id}
)

print(status_response.json())
```

### Q14: Error Handling and Monitoring
**Answer:**

```python
# Notebook error handling
try:
    # ETL logic
    df = spark.read.format("delta").load("/path/to/bronze")
    processed_df = transform_data(df)
    processed_df.write.format("delta").mode("append").save("/path/to/silver")
    
    dbutils.notebook.exit(json.dumps({
        "status": "success",
        "records_processed": processed_df.count()
    }))
    
except Exception as e:
    # Log error
    error_log = {
        "timestamp": datetime.now().isoformat(),
        "notebook": dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get(),
        "error": str(e),
        "stack_trace": traceback.format_exc()
    }
    
    # Write to error log table
    spark.createDataFrame([error_log]).write \
        .format("delta") \
        .mode("append") \
        .save("/path/to/error_logs")
    
    # Send alert
    send_alert(error_log)
    
    # Exit with error
    dbutils.notebook.exit(json.dumps({
        "status": "failed",
        "error": str(e)
    }))

# Job monitoring dashboard query
spark.sql("""
    SELECT 
        j.job_name,
        r.run_id,
        r.start_time,
        r.end_time,
        (r.end_time - r.start_time) / 1000 as duration_seconds,
        r.state.result_state as status,
        t.task_key,
        t.state.result_state as task_status,
        t.start_time as task_start,
        t.end_time as task_end
    FROM system.workflow.job_runs r
    JOIN system.workflow.jobs j ON r.job_id = j.job_id
    LATERAL VIEW explode(r.tasks) t
    WHERE r.start_time >= current_timestamp() - INTERVAL 7 DAYS
    ORDER BY r.start_time DESC
""")

# Set up alerting
def setup_job_alerts(job_id):
    """Configure alerts for job failures"""
    
    alert_config = {
        "job_id": job_id,
        "email_notifications": {
            "on_failure": ["oncall@company.com"],
            "on_success": ["team@company.com"]
        },
        "webhook_notifications": {
            "on_failure": [{
                "id": "slack_webhook",
                "url": "https://hooks.slack.com/services/XXX/YYY/ZZZ"
            }]
        }
    }
    
    requests.post(
        f"{workspace_url}/api/2.1/jobs/update",
        headers={"Authorization": f"Bearer {token}"},
        json=alert_config
    )
```

---

# Cost Optimization

### Q15: Cluster and Warehouse Optimization
**Answer:**

```python
# Cluster cost optimization strategies

# 1. Autoscaling
cluster_config = {
    "autoscale": {
        "min_workers": 2,
        "max_workers": 10
    },
    "autotermination_minutes": 10
}

# 2. Spot instances
cluster_config = {
    "aws_attributes": {
        "first_on_demand": 1,
        "availability": "SPOT_WITH_FALLBACK",
        "spot_bid_price_percent": 100
    }
}

# 3. Right-size instance types
# For memory-intensive: r5 instances
# For compute-intensive: c5 instances
# For balanced: m5 instances

# 4. Use instance pools
spark.sql("""
    CREATE INSTANCE POOL data_processing_pool
    USING INSTANCE_TYPE 'i3.2xlarge'
    WITH MIN_IDLE_INSTANCES 2
    MAX_CAPACITY 20
""")

# 5. Optimize write operations
df.write.format("delta") \
    .option("optimizeWrite", "true") \  # Combine small files during write
    .option("autoCompact", "true") \    # Auto-compact after write
    .save("/path/to/table")

# 6. Cache strategically
# Cache data used multiple times
df_cached = df.cache()
result1 = df_cached.filter(col("status") == "active").count()
result2 = df_cached.groupBy("category").count()
df_cached.unpersist()  # Clean up when done

# 7. Partition pruning
# Ensure predicates on partition columns
spark.sql("""
    SELECT * FROM orders
    WHERE order_date >= '2024-01-01'  -- Partition column
    AND status = 'completed'           -- Regular filter
""")

# Cost analysis query
spark.sql("""
    SELECT 
        workspace_id,
        usage_date,
        sku_name,
        SUM(usage_quantity) as total_dbu,
        SUM(usage_quantity * list_price) as estimated_cost
    FROM system.billing.usage
    WHERE usage_date >= current_date() - 30
    GROUP BY workspace_id, usage_date, sku_name
    ORDER BY estimated_cost DESC
""")

# Track job costs
spark.sql("""
    WITH job_costs AS (
        SELECT 
            j.job_name,
            r.run_id,
            r.start_time,
            r.end_time,
            (r.end_time - r.start_time) / 3600000.0 as runtime_hours,
            c.num_workers * runtime_hours * 0.75 as estimated_dbu_cost
        FROM system.workflow.job_runs r
        JOIN system.workflow.jobs j ON r.job_id = j.job_id
        JOIN system.compute.clusters c ON r.cluster_id = c.cluster_id
        WHERE r.start_time >= current_date() - 7
    )
    SELECT 
        job_name,
        COUNT(*) as run_count,
        SUM(runtime_hours) as total_runtime,
        SUM(estimated_dbu_cost) as total_cost,
        AVG(estimated_dbu_cost) as avg_cost_per_run
    FROM job_costs
    GROUP BY job_name
    ORDER BY total_cost DESC
""")
```

---

## 🚀 Best Practices Summary

### Delta Lake
- ✅ Use OPTIMIZE regularly
- ✅ Z-ORDER by frequently filtered columns
- ✅ Enable auto-optimize for production tables
- ✅ VACUUM old versions periodically
- ✅ Use MERGE for upserts instead of DELETE + INSERT

### Unity Catalog
- ✅ Organize data in catalogs (prod, dev, test)
- ✅ Use row/column-level security
- ✅ Enable audit logging
- ✅ Document tables and columns
- ✅ Use Delta Sharing for external access

### Performance
- ✅ Enable Photon for SQL workloads
- ✅ Use serverless SQL warehouses
- ✅ Cache frequently accessed data
- ✅ Partition large tables
- ✅ Use broadcast joins for small tables

### Cost
- ✅ Use autoscaling and autotermination
- ✅ Use spot instances where possible
- ✅ Right-size clusters
- ✅ Monitor DBU usage
- ✅ Clean up unused resources

---

Good luck with your Databricks interviews! ⚡

