# Databricks notebook source
# MAGIC %md
# MAGIC # RetailPulse — Real-Time Data Generator
# MAGIC
# MAGIC Simulates a retail POS system continuously emitting transaction events.
# MAGIC Each cycle writes one JSON file (a "micro-batch" of transactions) into a
# MAGIC Unity Catalog Volume. Autoloader (Part 2) treats this volume as its
# MAGIC streaming source, exactly like it would treat a real S3/ADLS landing zone.
# MAGIC
# MAGIC **Run this notebook in one cluster session while you build/test Bronze in
# MAGIC another notebook** — that's what makes the "real-time" simulation real:
# MAGIC files keep landing while your streaming query is live.

# COMMAND ----------

# MAGIC %pip install faker --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import json
import random
import time
import uuid
from datetime import datetime, timezone

from faker import Faker

fake = Faker()
Faker.seed(42)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration
# MAGIC
# MAGIC `LANDING_PATH` must point at the Volume you created in `00_setup_catalog`.
# MAGIC Adjust `CATALOG` / `SCHEMA` widgets if your workspace's default catalog
# MAGIC differs from `retailpulse`.

# COMMAND ----------

dbutils.widgets.text("catalog", "retailpulse", "Catalog")
dbutils.widgets.text("schema", "lakehouse", "Schema")
dbutils.widgets.text("batch_size", "25", "Records per file")
dbutils.widgets.text("interval_seconds", "10", "Seconds between files")
dbutils.widgets.text("num_batches", "60", "Number of files to write")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
BATCH_SIZE = int(dbutils.widgets.get("batch_size"))
INTERVAL_SECONDS = int(dbutils.widgets.get("interval_seconds"))
NUM_BATCHES = int(dbutils.widgets.get("num_batches"))

LANDING_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/landing_zone"

print(f"Writing to: {LANDING_PATH}")
print(f"{NUM_BATCHES} files x {BATCH_SIZE} records, every {INTERVAL_SECONDS}s")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reference data
# MAGIC
# MAGIC Fixed pools for products and stores so Silver/Gold aggregations (Parts 3-4)
# MAGIC have stable dimensions to group and join against, instead of fully random
# MAGIC noise on every run.

# COMMAND ----------

CATEGORIES = {
    "Electronics": ["Wireless Earbuds", "Smartphone Case", "Power Bank", "Bluetooth Speaker", "USB-C Cable"],
    "Grocery": ["Basmati Rice 5kg", "Olive Oil 1L", "Whole Wheat Atta", "Green Tea Box", "Almonds 500g"],
    "Apparel": ["Cotton T-Shirt", "Denim Jeans", "Running Shoes", "Formal Shirt", "Winter Jacket"],
    "Home & Kitchen": ["Non-Stick Pan", "Mixer Grinder", "Bedsheet Set", "LED Bulb Pack", "Storage Containers"],
    "Beauty": ["Face Wash", "Sunscreen SPF50", "Shampoo 400ml", "Lip Balm", "Hair Serum"],
}

STORES = [
    {"store_id": "STR-BLR-01", "city": "Bengaluru"},
    {"store_id": "STR-HYD-01", "city": "Hyderabad"},
    {"store_id": "STR-PUN-01", "city": "Pune"},
    {"store_id": "STR-CHN-01", "city": "Chennai"},
    {"store_id": "STR-BBS-01", "city": "Bhubaneswar"},
]

PAYMENT_METHODS = ["UPI", "Credit Card", "Debit Card", "Net Banking", "Cash"]
CHANNELS = ["In-Store", "Online", "Mobile App"]
SEGMENTS = ["New", "Regular", "Premium"]

CUSTOMER_POOL = [f"CUST-{i:05d}" for i in range(1, 2001)]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Record generator
# MAGIC
# MAGIC `dirty_data_rate` controls how often a record is deliberately flawed —
# MAGIC null customer_id, a duplicated transaction_id, or a negative quantity.
# MAGIC This mirrors what real POS/ETL feeds actually look like and gives Part 3
# MAGIC genuine data quality problems to solve.

# COMMAND ----------

_last_transaction_id = None


def generate_transaction(dirty_data_rate: float = 0.05) -> dict:
    global _last_transaction_id

    category = random.choice(list(CATEGORIES.keys()))
    product_name = random.choice(CATEGORIES[category])
    store = random.choice(STORES)
    quantity = random.randint(1, 5)
    unit_price = round(random.uniform(99, 4999), 2)
    discount_pct = random.choice([0, 0, 0, 5, 10, 15, 20])
    gross = quantity * unit_price
    total_amount = round(gross * (1 - discount_pct / 100), 2)

    transaction_id = str(uuid.uuid4())
    is_dirty = random.random() < dirty_data_rate

    record = {
        "transaction_id": transaction_id,
        "transaction_timestamp": datetime.now(timezone.utc).isoformat(),
        "store_id": store["store_id"],
        "store_city": store["city"],
        "customer_id": random.choice(CUSTOMER_POOL),
        "customer_segment": random.choice(SEGMENTS),
        "product_name": product_name,
        "category": category,
        "quantity": quantity,
        "unit_price": unit_price,
        "discount_percent": discount_pct,
        "total_amount": total_amount,
        "payment_method": random.choice(PAYMENT_METHODS),
        "channel": random.choice(CHANNELS),
    }

    if is_dirty:
        glitch = random.choice(["null_customer", "duplicate_id", "negative_qty"])
        if glitch == "null_customer":
            record["customer_id"] = None
        elif glitch == "duplicate_id" and _last_transaction_id:
            record["transaction_id"] = _last_transaction_id  # forces a duplicate
        elif glitch == "negative_qty":
            record["quantity"] = -record["quantity"]

    _last_transaction_id = transaction_id
    return record

# COMMAND ----------

# MAGIC %md
# MAGIC ## Streaming write loop
# MAGIC
# MAGIC Writes one JSON-lines file per cycle directly to the Volume using plain
# MAGIC Python file I/O (Volumes are POSIX-accessible at `/Volumes/...`, no `dbutils`
# MAGIC needed for the write itself). This is what "continuously arriving files"
# MAGIC looks like from Autoloader's point of view.

# COMMAND ----------

dbutils.fs.mkdirs(LANDING_PATH)

for batch_num in range(1, NUM_BATCHES + 1):
    records = [generate_transaction() for _ in range(BATCH_SIZE)]

    file_name = f"transactions_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S_%f')}.json"
    file_path = f"{LANDING_PATH}/{file_name}"

    with open(file_path, "w") as f:
        for record in records:
            f.write(json.dumps(record) + "\n")

    print(f"[{batch_num}/{NUM_BATCHES}] wrote {len(records)} records -> {file_name}")
    time.sleep(INTERVAL_SECONDS)

print("Generator finished. Re-run the cell (or this notebook) to keep the feed alive.")
