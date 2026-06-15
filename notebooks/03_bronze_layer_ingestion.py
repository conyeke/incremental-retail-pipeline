from pyspark.sql import functions as F

# Paths
landing_path = "/Volumes/workspace/default/project/landing"
bronze_checkpoint_path = "/Volumes/workspace/default/project/checkpoints/bronze_sales"
bronze_schema_path = "/Volumes/workspace/default/project/checkpoints/bronze_sales_schema" 
bronze_table_path = "/Volumes/workspace/default/project/delta/bronze_sales"

# 1. Read newly arrived files incrementally using Auto Loader
raw_stream = spark.readStream \
    .format("cloudFiles") \
    .option("cloudFiles.format", "csv") \
    .option("cloudFiles.inferColumnSchema", "true") \
    .option("header", "true") \
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns") \
    .option("cloudFiles.schemaLocation", bronze_schema_path) \
    .option("cloudFiles.rescuedDataColumn", "_rescued_data") \
    .option("cloudFiles.validateOptions", "false") \
    .load(landing_path)

# 2. Add ingestion metadata (Unity Catalog compliant)
bronze_df = raw_stream \
    .withColumn("ingested_at", F.current_timestamp()) \
    .withColumn("input_file_name", F.col("_metadata.file_path"))

# 3. Append the new data to the Bronze Delta table
query = bronze_df.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", bronze_checkpoint_path) \
    .option("mergeSchema", "true") \
    .trigger(availableNow=True) \
    .toTable("workspace.default.bronze_sales")

query.awaitTermination()
print("Bronze layer update complete: New data appended successfully.")