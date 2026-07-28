# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE gold_claims_by_region AS
# MAGIC SELECT c.state, COUNT(cl.claim_id) as total_claims, SUM(cl.claim_amount) as total_claim_amt
# MAGIC FROM silver_claims cl
# MAGIC JOIN silver_policies p ON cl.policy_id = p.policy_id
# MAGIC JOIN silver_customers c ON p.customer_id = c.customer_id
# MAGIC GROUP BY c.state;
# MAGIC
# MAGIC CREATE OR REPLACE TABLE gold_loss_ratio AS
# MAGIC SELECT p.policy_type, SUM(cl.claim_amount)/SUM(p.premium) as loss_ratio
# MAGIC FROM silver_claims cl JOIN silver_policies p ON cl.policy_id = p.policy_id
# MAGIC GROUP BY p.policy_type;
# MAGIC
# MAGIC CREATE OR REPLACE TABLE gold_fraud_summary AS
# MAGIC SELECT status, fraud_flag, COUNT(*) as cnt FROM silver_claims GROUP BY status, fraud_flag;

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from gold_claims_by_region

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from gold_fraud_summary

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from gold_loss_ratio

# COMMAND ----------

display(spark.table("gold_claims_by_region"))

# COMMAND ----------

display(spark.table("gold_fraud_summary"))

# COMMAND ----------

display(spark.table("gold_loss_ratio"))