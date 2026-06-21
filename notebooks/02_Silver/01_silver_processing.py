# Databricks notebook source
# MAGIC %md
# MAGIC # RetailPulse — Silver Layer: Data Quality, Dedup, MERGE Upserts
# MAGIC
# MAGIC Streams from the Bronze Delta table, validates each record against
# MAGIC business rules, deduplicates corrected/repeated records by
# MAGIC `transaction_id`, and upserts the result into Silver via `MERGE`.
# MAGIC Records that fail validation are routed to a quarantine table instead
# MAGIC of being dropped.
# MAGIC
# MAGIC Run `01_bronze_ingestion` at least once before this, so there's data in
# MAGIC Bronze to stream from.

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

dbutils.widgets.text("catalog", "retailpulse", "Catalog")
dbutils.widgets.text("schema", "lakehouse", "Schema")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")

BRONZE_TABLE = f"{CATALOG}.{SCHEMA}.bronze_transactions"
SILVER_TABLE = f"{CATALOG}.{SCHEMA}.silver_transactions"
QUARANTINE_TABLE = f"{CATALOG}.{SCHEMA}.silver_transactions_quarantine"
CHECKPOINT_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/checkpoints/silver_transactions"

print(f"Source:     {BRONZE_TABLE}")
print(f"Silver:     {SILVER_TABLE}")
print(f"Quarantine: {QUARANTINE_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data quality rules
# MAGIC
# MAGIC Each rule is a (name, failure_condition) pair. A record can fail more
# MAGIC than one rule — we capture *all* of them in a `dq_failures` array rather
# MAGIC than stopping at the first match, so quarantine analysis later tells you
# MAGIC exactly what's wrong, not just that something is.

# COMMAND ----------

VALID_PAYMENT_METHODS = ["UPI", "Credit Card", "Debit Card", "Net Banking", "Cash"]
VALID_CATEGORIES = ["Electronics", "Grocery", "Apparel", "Home & Kitchen", "Beauty"]

DQ_RULES = [
    ("missing_transaction_id", F.col("transaction_id").isNull()),
    ("missing_customer_id", F.col("customer_id").isNull()),
    ("non_positive_quantity", F.col("quantity") <= 0),
    ("non_positive_amount", F.col("total_amount") <= 0),
    ("invalid_payment_method", ~F.col("payment_method").isin(VALID_PAYMENT_METHODS)),
    ("invalid_category", ~F.col("category").isin(VALID_CATEGORIES)),
]


def add_quality_checks(df):
    failure_exprs = [F.when(cond, F.lit(name)) for name, cond in DQ_RULES]
    df = df.withColumn(
        "dq_failures",
        F.array_except(F.array(*failure_exprs), F.array(F.lit(None).cast("string"))),
    )
    return df.withColumn("is_valid", F.size("dq_failures") == 0)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ensure Silver and quarantine tables exist
# MAGIC
# MAGIC `MERGE` requires a target table to already exist. We derive the schema
# MAGIC from Bronze (plus our quality columns) once, idempotently, rather than
# MAGIC hardcoding a schema that could drift out of sync.

# COMMAND ----------

bronze_sample = add_quality_checks(spark.read.table(BRONZE_TABLE).limit(0))

if not spark.catalog.tableExists(SILVER_TABLE):
    (
        bronze_sample.drop("dq_failures", "is_valid")
        .write.format("delta")
        .saveAsTable(SILVER_TABLE)
    )
    print(f"Created {SILVER_TABLE}")

if not spark.catalog.tableExists(QUARANTINE_TABLE):
    (bronze_sample.write.format("delta").saveAsTable(QUARANTINE_TABLE))
    print(f"Created {QUARANTINE_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Per-batch logic: dedup, split, MERGE
# MAGIC
# MAGIC `dedup_batch` keeps one row per `transaction_id` per micro-batch, the
# MAGIC most recent by `transaction_timestamp`. Cross-batch duplicates (a
# MAGIC correction arriving in a *later* batch) are still handled correctly —
# MAGIC that's what the `MERGE` below is for, since it matches against the
# MAGIC entire Silver table, not just the current batch.
# MAGIC
# MAGIC **Alternative for very high-throughput streams:** native
# MAGIC `dropDuplicatesWithinWatermark` directly on the streaming DataFrame. It's
# MAGIC cheaper at scale, but only catches duplicates that arrive within the
# MAGIC watermark window — a correction arriving after the window closes would
# MAGIC be missed. We use the `foreachBatch` + `MERGE` approach here because
# MAGIC correctness across arbitrarily late corrections matters more than raw
# MAGIC throughput for this use case.

# COMMAND ----------

def dedup_batch(df):
    window = Window.partitionBy("transaction_id").orderBy(
        F.col("transaction_timestamp").desc(), F.col("_ingest_timestamp").desc()
    )
    return (
        df.withColumn("rn", F.row_number().over(window))
        .filter("rn = 1")
        .drop("rn")
    )


def upsert_to_silver(batch_df, batch_id):
    if batch_df.isEmpty():
        return

    deduped = dedup_batch(batch_df)
    valid_df = deduped.filter("is_valid").drop("dq_failures", "is_valid")
    quarantine_df = deduped.filter("NOT is_valid")

    print(f"Batch {batch_id}: {valid_df.count()} valid, {quarantine_df.count()} quarantined")

    (
        DeltaTable.forName(spark, SILVER_TABLE)
        .alias("t")
        .merge(valid_df.alias("s"), "t.transaction_id = s.transaction_id")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

    if not quarantine_df.isEmpty():
        quarantine_df.write.format("delta").mode("append").saveAsTable(QUARANTINE_TABLE)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Bronze as a stream, apply checks, write via foreachBatch

# COMMAND ----------

bronze_stream = spark.readStream.table(BRONZE_TABLE)
checked_stream = add_quality_checks(bronze_stream)

query = (
    checked_stream.writeStream
        .foreachBatch(upsert_to_silver)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .trigger(availableNow=True)
        .start()
)
query.awaitTermination()
print("Silver batch complete.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Silver

# COMMAND ----------

silver = spark.table(SILVER_TABLE)
print(f"Silver row count: {silver.count()}")

dupe_check = silver.groupBy("transaction_id").count().filter("count > 1")
print(f"Duplicate transaction_ids remaining in Silver: {dupe_check.count()}")

display(silver.orderBy(F.col("_ingest_timestamp").desc()).limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Inspect quarantine — what's actually failing, and why

# COMMAND ----------

quarantine = spark.table(QUARANTINE_TABLE)
print(f"Quarantined row count: {quarantine.count()}")

(
    quarantine
    .select(F.explode("dq_failures").alias("failure_reason"))
    .groupBy("failure_reason")
    .count()
    .orderBy(F.col("count").desc())
    .display()
)
