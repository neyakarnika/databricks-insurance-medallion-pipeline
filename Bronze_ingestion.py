# Databricks notebook source
!pip install faker
from faker import Faker
import random, csv
fake = Faker()

# Customers
customers = [(i, fake.name(), fake.city(), fake.state_abbr(), random.randint(18,75)) for i in range(1,2001)]
# Policies
policies = [(i, random.randint(1,2000), random.choice(['Auto','Health','Life','Home']),
             round(random.uniform(2000,15000),2), fake.date_between(start_date='-3y', end_date='today')) for i in range(1,3001)]
# Claims
claims = [(i, random.randint(1,3000), round(random.uniform(500,20000),2),
           fake.date_between(start_date='-2y', end_date='today'),
           random.choice(['Approved','Rejected','Pending']),
           random.choice([0,0,0,1])) for i in range(1,5001)]  # last field = fraud_flag

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

cust_df = spark.createDataFrame(customers, ["customer_id","name","city","state","age"])
pol_df = spark.createDataFrame(policies, ["policy_id","customer_id","policy_type","premium","start_date"])
claims_df = spark.createDataFrame(claims, ["claim_id","policy_id","claim_amount","claim_date","status","fraud_flag"])

for name, df in [("customers",cust_df), ("policies",pol_df), ("claims",claims_df)]:
    df = df.withColumn("ingestion_ts", current_timestamp()).withColumn("source_system", lit("synthetic_gen"))
    df.write.format("delta").mode("overwrite").saveAsTable(f"bronze_{name}")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM bronze_claims LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM bronze_customers;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM bronze_policies;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM bronze_claims;