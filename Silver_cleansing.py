# Databricks notebook source
# MAGIC %pip install faker
# MAGIC from pyspark.sql.functions import col, to_date
# MAGIC
# MAGIC bronze_claims = spark.table("bronze_claims").dropDuplicates(["claim_id"])
# MAGIC bronze_claims = bronze_claims.withColumn("claim_date", to_date("claim_date")) \
# MAGIC                               .filter(col("claim_amount").isNotNull())

# COMMAND ----------

from pyspark.sql.functions import col, to_date, current_timestamp

# --- Silver: Customers ---
silver_customers = spark.table("bronze_customers").dropDuplicates(["customer_id"]) \
    .filter(col("customer_id").isNotNull()) \
    .withColumn("age", col("age").cast("int")) \
    .filter((col("age") >= 18) & (col("age") <= 100))

silver_customers.write.format("delta").mode("overwrite").saveAsTable("silver_customers")

# --- Silver: Policies ---
silver_policies = spark.table("bronze_policies").dropDuplicates(["policy_id"]) \
    .filter(col("policy_id").isNotNull()) \
    .withColumn("start_date", to_date("start_date")) \
    .withColumn("premium", col("premium").cast("double")) \
    .filter(col("premium") > 0)

silver_policies.write.format("delta").mode("overwrite").saveAsTable("silver_policies")

# --- Silver: Claims ---
silver_claims = spark.table("bronze_claims").dropDuplicates(["claim_id"]) \
    .filter(col("claim_id").isNotNull()) \
    .withColumn("claim_date", to_date("claim_date")) \
    .withColumn("claim_amount", col("claim_amount").cast("double")) \
    .filter(col("claim_amount").isNotNull())

silver_claims.write.format("delta").mode("overwrite").saveAsTable("silver_claims")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM silver_customers;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM silver_policies;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM silver_claims;

# COMMAND ----------



import random
from pyspark.sql.functions import current_timestamp, lit
from faker import Faker
fake = Faker()

# Simulate: some updates to existing claims (status changes) + some brand new claims
existing_ids = [row.claim_id for row in spark.table("silver_claims").select("claim_id").sample(0.05).collect()]

updates = [(cid, random.randint(1,3000), round(random.uniform(500,20000),2),
            fake.date_between(start_date='-2y', end_date='today'),
            random.choice(['Approved','Rejected']),  # status change
            random.choice([0,1])) for cid in existing_ids]

new_claims = [(i, random.randint(1,3000), round(random.uniform(500,20000),2),
               fake.date_between(start_date='-30d', end_date='today'),
               random.choice(['Approved','Rejected','Pending']),
               random.choice([0,0,0,1])) for i in range(5001, 5201)]  # new IDs beyond existing max

incremental_batch = spark.createDataFrame(updates + new_claims,
    ["claim_id","policy_id","claim_amount","claim_date","status","fraud_flag"]) \
    .withColumn("ingestion_ts", current_timestamp()) \
    .withColumn("source_system", lit("synthetic_gen_incremental"))

incremental_batch.createOrReplaceTempView("bronze_claims_incremental")

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO silver_claims tgt
# MAGIC USING bronze_claims_incremental src
# MAGIC ON tgt.claim_id = src.claim_id
# MAGIC WHEN MATCHED THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL silver_claims;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY silver_claims;

# COMMAND ----------

from pyspark.sql.functions import col

print("=== DATA QUALITY REPORT ===\n")

# --- 1. Row count comparison: Bronze vs Silver ---
for name in ["customers", "policies", "claims"]:
    bronze_cnt = spark.table(f"bronze_{name}").count()
    silver_cnt = spark.table(f"silver_{name}").count()
    dropped = bronze_cnt - silver_cnt
    print(f"[{name.upper()}] Bronze: {bronze_cnt} | Silver: {silver_cnt} | Dropped in cleansing: {dropped}")

print("\n=== NULL CHECKS (Silver) ===\n")

# --- 2. Null checks on key columns ---
null_checks = {
    "silver_customers": ["customer_id", "age"],
    "silver_policies": ["policy_id", "customer_id", "premium"],
    "silver_claims": ["claim_id", "policy_id", "claim_amount"]
}

for table, cols_to_check in null_checks.items():
    df = spark.table(table)
    for c in cols_to_check:
        null_count = df.filter(col(c).isNull()).count()
        status = "OK" if null_count == 0 else "FAIL"
        print(f"[{status}] {table}.{c} → {null_count} nulls")

print("\n=== REFERENTIAL INTEGRITY CHECK ===\n")

# --- 3. Every claim's policy_id must exist in silver_policies ---
orphan_claims = spark.table("silver_claims").join(
    spark.table("silver_policies"),
    on="policy_id",
    how="left_anti"
)
orphan_count = orphan_claims.count()
status = "OK" if orphan_count == 0 else "FAIL"
print(f"[{status}] Orphan claims (policy_id not in silver_policies): {orphan_count}")

# --- 4. Every policy's customer_id must exist in silver_customers ---
orphan_policies = spark.table("silver_policies").join(
    spark.table("silver_customers"),
    on="customer_id",
    how="left_anti"
)
orphan_count2 = orphan_policies.count()
status2 = "OK" if orphan_count2 == 0 else "FAIL"
print(f"[{status2}] Orphan policies (customer_id not in silver_customers): {orphan_count2}")