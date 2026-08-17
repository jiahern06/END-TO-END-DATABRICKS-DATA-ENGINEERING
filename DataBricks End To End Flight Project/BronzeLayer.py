# Databricks notebook source
# MAGIC %md
# MAGIC ### INCREMENTAL DATA INGESTION

# COMMAND ----------

dbutils.widgets.text("src","")

# COMMAND ----------

src_value = dbutils.widgets.get('src')

# COMMAND ----------

df= spark.readStream.format("cloudFiles") \
  .option("cloudFiles.format", "csv") \
  .option("cloudFiles.schemaLocation",f"/Volumes/workspace/bronze/bronzevolume/{src_value}/checkpoint")\
      .option("cloudFiles.schemaEvolutionMode","rescue")\
          .load(f"/Volumes/workspace/raw/rawvolume/rawdata/{src_value}/") 

# COMMAND ----------

df.writeStream.format("delta")\
    .outputMode("append")\
        .trigger(once=True)\
        .option("checkpointLocation",f"/Volumes/workspace/bronze/bronzevolume/{src_value}/checkpoints")\
            .option("path", f"/Volumes/workspace/bronze/bronzevolume/{src_value}/data")\
                .start()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM delta.`/Volumes/workspace/bronze/bronzevolume/bookings/data`

# COMMAND ----------

# MAGIC %md
# MAGIC  make it dynamic so we dont need to individually create notebooks

# COMMAND ----------



# COMMAND ----------

