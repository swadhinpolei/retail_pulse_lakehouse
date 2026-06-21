# Databricks notebook source
# MAGIC %md
# MAGIC # RetailPulse — Bronze Layer: Autoloader Ingestion
# MAGIC
# MAGIC Incrementally ingests raw transaction JSON files from the landing volume
# MAGIC into a Bronze Delta table using Auto Loader (`cloudFiles`). This layer is
# MAGIC append-only and intentionally untransformed — it exists to preserve a
# MAGIC faithful, replayable copy of exactly what the source system sent.
# MAGIC
# MAGIC Run `00_setup_catalog` and start `01_realtime_data_generator` (in another
# MAGIC notebook / cluster session) before running this one, so there's something
# MAGIC for Auto Loader to pick up.

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

dbutils.widgets.text("catalog", "retailpulse", "Catalog")
dbutils.widgets.text("schema", "lakehouse", "Schema")
dbutils.widgets.dropdown("run_mode", "available_now", ["available_now", "continuous_demo"], "Run mode")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
RUN_MODE = dbutils.widgets.get("run_mode")

LANDING_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/landing_zone"
SCHEMA_LOCATION = f"/Volumes/{CATALOG}/{SCHEMA}/checkpoints/bronze_transactions_schema"
CHECKPOINT_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/checkpoints/bronze_transactions"
BRONZE_TABLE = f"{CATALOG}.{SCHEMA}.bronze_transactions"

print(f"Source:     {LANDING_PATH}")
print(f"Checkpoint: {CHECKPOINT_PATH}")
print(f"Target:     {BRONZE_TABLE}")
print(f"Run mode:   {RUN_MODE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read stream — Auto Loader over the landing volume
# MAGIC
# MAGIC `inferColumnTypes` lets Auto Loader infer real types (double, timestamp)
# MAGIC instead of leaving everything as string. `schemaEvolutionMode=addNewColumns`
# MAGIC is the production-safe default: new fields get added automatically, but
# MAGIC the stream halts and asks you to restart it so you consciously acknowledge
# MAGIC the schema change rather than having it pass silently.

# COMMAND ----------

raw_stream = (
    spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", SCHEMA_LOCATION)
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("cloudFiles.inferColumnTypes", "true")
        .load(LANDING_PATH)
)

bronze_df = (
    raw_stream
        .withColumn("_ingest_timestamp", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write stream — append-only to the Bronze Delta table
# MAGIC
# MAGIC Two run modes, both production patterns, used in different contexts:
# MAGIC
# MAGIC - **available_now**: drains everything currently in the source, then stops.
# MAGIC   This is what you'd schedule via Databricks Workflows in production —
# MAGIC   no idle cluster cost between runs. Re-running this cell repeatedly while
# MAGIC   your generator is producing files simulates a frequently-scheduled job.
# MAGIC - **continuous_demo**: a genuine 10-second micro-batch stream, for watching
# MAGIC   ingestion happen live. Stop it manually (next cell) when you're done
# MAGIC   watching — don't leave it running, it consumes compute the whole time.

# COMMAND ----------

writer = (
    bronze_df.writeStream
        .format("delta")
        .option("checkpointLocation", CHECKPOINT_PATH)
        .option("mergeSchema", "true")
        .outputMode("append")
)

if RUN_MODE == "available_now":
    query = writer.trigger(availableNow=True).toTable(BRONZE_TABLE)
    query.awaitTermination()
    print("Batch drained. Stream stopped itself (availableNow).")
else:
    query = writer.trigger(processingTime="10 seconds").toTable(BRONZE_TABLE)
    print("Streaming continuously. Run the next cell to stop it when you're done watching.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stop the stream (only needed for continuous_demo mode)

# COMMAND ----------

for s in spark.streams.active:
    print(f"Stopping: {s.name or s.id}")
    s.stop()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify the Bronze table

# COMMAND ----------

bronze = spark.table(BRONZE_TABLE)

print(f"Row count: {bronze.count()}")
display(bronze.orderBy(F.col("_ingest_timestamp").desc()).limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check for rescued / malformed data
# MAGIC
# MAGIC Anything Auto Loader couldn't reconcile with the inferred schema lands
# MAGIC here instead of crashing the pipeline or being silently dropped. In a
# MAGIC real production pipeline you'd alert on this growing unexpectedly.

# COMMAND ----------

rescued_count = bronze.filter(F.col("_rescued_data").isNotNull()).count()
print(f"Rows with rescued data: {rescued_count}")

if rescued_count > 0:
    display(bronze.filter(F.col("_rescued_data").isNotNull()).limit(5))
