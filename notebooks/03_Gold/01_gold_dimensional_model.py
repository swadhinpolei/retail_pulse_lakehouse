# Databricks notebook source
# MAGIC %md
# MAGIC # RetailPulse — Gold Layer: Dimensional Model & Business Aggregates
# MAGIC
# MAGIC Builds a star schema (dim_store, dim_product, dim_customer, fact_sales)
# MAGIC from Silver, then derives two pre-aggregated marts for dashboarding.
# MAGIC This runs as a periodic batch job, not a stream — see notebook
# MAGIC discussion for why.
# MAGIC
# MAGIC Run `02_silver/01_silver_processing` at least once before this.

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

dbutils.widgets.text("catalog", "retailpulse", "Catalog")
dbutils.widgets.text("schema", "lakehouse", "Schema")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")

SILVER_TABLE = f"{CATALOG}.{SCHEMA}.silver_transactions"
DIM_STORE = f"{CATALOG}.{SCHEMA}.dim_store"
DIM_PRODUCT = f"{CATALOG}.{SCHEMA}.dim_product"
DIM_CUSTOMER = f"{CATALOG}.{SCHEMA}.dim_customer"
FACT_SALES = f"{CATALOG}.{SCHEMA}.fact_sales"
GOLD_CATEGORY_DAILY = f"{CATALOG}.{SCHEMA}.gold_daily_sales_by_category"
GOLD_STORE_DAILY = f"{CATALOG}.{SCHEMA}.gold_daily_sales_by_store"

# COMMAND ----------

# MAGIC %md
# MAGIC ## DDL — dimension and fact tables with surrogate keys
# MAGIC
# MAGIC Defined explicitly via DDL (not inferred from a DataFrame) because Gold
# MAGIC schemas should be deliberate, governed contracts — and `IDENTITY` columns
# MAGIC can only be declared this way.

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {DIM_STORE} (
    store_key BIGINT GENERATED ALWAYS AS IDENTITY,
    store_id STRING,
    store_city STRING,
    _updated_at TIMESTAMP
) USING DELTA
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {DIM_PRODUCT} (
    product_key BIGINT GENERATED ALWAYS AS IDENTITY,
    product_name STRING,
    category STRING,
    _updated_at TIMESTAMP
) USING DELTA
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {DIM_CUSTOMER} (
    customer_key BIGINT GENERATED ALWAYS AS IDENTITY,
    customer_id STRING,
    customer_segment STRING,
    _updated_at TIMESTAMP
) USING DELTA
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {FACT_SALES} (
    transaction_id STRING,
    store_key BIGINT,
    product_key BIGINT,
    customer_key BIGINT,
    transaction_timestamp TIMESTAMP,
    transaction_date DATE,
    quantity INT,
    unit_price DOUBLE,
    discount_percent INT,
    total_amount DOUBLE,
    payment_method STRING,
    channel STRING,
    _updated_at TIMESTAMP
) USING DELTA
""")

print("Dimension and fact tables ready.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generic dimension upsert
# MAGIC
# MAGIC One function for all three dimensions: match on natural key, update
# MAGIC descriptive attributes if matched (SCD Type 1), insert a new row — and a
# MAGIC new surrogate key — if not. The surrogate key column is deliberately
# MAGIC excluded from both the update and insert value maps so Delta auto-assigns
# MAGIC it only for genuinely new rows.

# COMMAND ----------

def upsert_dimension(table_name, source_df, natural_key_cols, attribute_cols):
    target = DeltaTable.forName(spark, table_name)
    merge_condition = " AND ".join(f"t.{c} = s.{c}" for c in natural_key_cols)

    update_set = {c: f"s.{c}" for c in attribute_cols}
    update_set["_updated_at"] = "current_timestamp()"

    insert_values = {c: f"s.{c}" for c in natural_key_cols + attribute_cols}
    insert_values["_updated_at"] = "current_timestamp()"

    (
        target.alias("t")
        .merge(source_df.alias("s"), merge_condition)
        .whenMatchedUpdate(set=update_set)
        .whenNotMatchedInsert(values=insert_values)
        .execute()
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build the dimensions from Silver

# COMMAND ----------

silver = spark.table(SILVER_TABLE)

store_source = silver.select("store_id", "store_city").distinct()
upsert_dimension(DIM_STORE, store_source, ["store_id"], ["store_city"])

product_source = silver.select("product_name", "category").distinct()
upsert_dimension(DIM_PRODUCT, product_source, ["product_name", "category"], [])

# customer_segment isn't a stable attribute in our simulated data (it's random
# per transaction) — we take the most recent value per customer as a Type 1
# stand-in. A real CRM-sourced segment would be far more stable than this.
latest_segment_window = Window.partitionBy("customer_id").orderBy(F.col("transaction_timestamp").desc())
customer_source = (
    silver
    .withColumn("rn", F.row_number().over(latest_segment_window))
    .filter("rn = 1")
    .select("customer_id", "customer_segment")
)
upsert_dimension(DIM_CUSTOMER, customer_source, ["customer_id"], ["customer_segment"])

print("Dimensions upserted.")
print(f"  dim_store:    {spark.table(DIM_STORE).count()} rows")
print(f"  dim_product:  {spark.table(DIM_PRODUCT).count()} rows")
print(f"  dim_customer: {spark.table(DIM_CUSTOMER).count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build fact_sales
# MAGIC
# MAGIC Join Silver against the (now up-to-date) dimensions to resolve surrogate
# MAGIC keys, then MERGE into the fact table keyed on `transaction_id` — same
# MAGIC idempotency guarantee as Silver: re-running this notebook updates
# MAGIC existing facts rather than duplicating them.

# COMMAND ----------

dim_store_df = spark.table(DIM_STORE)
dim_product_df = spark.table(DIM_PRODUCT)
dim_customer_df = spark.table(DIM_CUSTOMER)

fact_source = (
    silver
    .join(dim_store_df, on="store_id", how="left")
    .join(dim_product_df, on=["product_name", "category"], how="left")
    .join(dim_customer_df, on="customer_id", how="left")
    .withColumn("transaction_date", F.to_date("transaction_timestamp"))
    .select(
        "transaction_id", "store_key", "product_key", "customer_key",
        "transaction_timestamp", "transaction_date", "quantity", "unit_price",
        "discount_percent", "total_amount", "payment_method", "channel",
    )
    .withColumn("_updated_at", F.current_timestamp())
)

(
    DeltaTable.forName(spark, FACT_SALES)
    .alias("t")
    .merge(fact_source.alias("s"), "t.transaction_id = s.transaction_id")
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
)

print(f"fact_sales: {spark.table(FACT_SALES).count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Business aggregate marts
# MAGIC
# MAGIC Pre-joined, pre-aggregated, dashboard-ready. `overwrite` is the right
# MAGIC call here: these are fully recomputed from the fact table each run,
# MAGIC which is simple and correct at this volume. At larger scale you'd switch
# MAGIC to incremental aggregation (merge by grain key, only touching
# MAGIC partitions affected by new fact rows) — a Part 7 performance topic.

# COMMAND ----------

fact = spark.table(FACT_SALES)

category_daily = (
    fact.join(dim_product_df, "product_key")
    .groupBy("transaction_date", "category")
    .agg(
        F.sum("total_amount").alias("total_revenue"),
        F.sum("quantity").alias("total_units"),
        F.count("transaction_id").alias("total_transactions"),
    )
)
category_daily.write.format("delta").mode("overwrite").saveAsTable(GOLD_CATEGORY_DAILY)

store_daily = (
    fact.join(dim_store_df, "store_key")
    .groupBy("transaction_date", "store_id", "store_city")
    .agg(
        F.sum("total_amount").alias("total_revenue"),
        F.count("transaction_id").alias("total_transactions"),
        F.countDistinct("customer_key").alias("unique_customers"),
    )
)
store_daily.write.format("delta").mode("overwrite").saveAsTable(GOLD_STORE_DAILY)

print(f"gold_daily_sales_by_category: {category_daily.count()} rows")
print(f"gold_daily_sales_by_store:    {store_daily.count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify

# COMMAND ----------

print("-- Top categories by revenue --")
spark.table(GOLD_CATEGORY_DAILY).groupBy("category").agg(
    F.sum("total_revenue").alias("revenue")
).orderBy(F.col("revenue").desc()).display()

print("-- Store performance --")
spark.table(GOLD_STORE_DAILY).groupBy("store_id", "store_city").agg(
    F.sum("total_revenue").alias("revenue"),
    F.sum("total_transactions").alias("transactions"),
).orderBy(F.col("revenue").desc()).display()
