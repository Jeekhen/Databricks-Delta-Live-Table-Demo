# Databricks notebook source
# MAGIC %fs mkdirs /home/jk.wong@dataricks.com/dlt_demo

# COMMAND ----------

# MAGIC %fs
# MAGIC 
# MAGIC ls /home/jk.wong@dataricks.com/dlt_demo

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC use jk_wong_data;
# MAGIC show views;

# COMMAND ----------

# MAGIC %sql
# MAGIC use jk_wong_data;
# MAGIC select * from GL_new_loan_balances_by_country;

# COMMAND ----------


