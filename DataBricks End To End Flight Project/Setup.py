# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE VOLUME workspace.raw.rawvolume

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM delta.`/Volumes/workspace/bronze/bronzevolume/flights/data/`