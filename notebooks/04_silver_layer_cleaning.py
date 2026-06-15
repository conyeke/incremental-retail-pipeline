from pyspark.sql import functions as F
from delta.tables import DeltaTable
from pyspark.sql.window import Window

# Configuration
bronze_table_name = "workspace.default.bronze_sales"
silver_table_name = "workspace.default.silver_sales"
silver_checkpoint_path = "/Volumes/workspace/default/project/checkpoints/silver_sales"

# 1. Read incrementally from the Bronze Delta Table
bronze_stream = spark.readStream.table(bronze_table_name)

# 2. UCI Online Retail Specific Data Cleaning and Transformations
silver_transformed_df = (
    bronze_stream
    .withColumn("Quantity", F.col("Quantity").cast("integer"))
    .withColumn("UnitPrice", F.col("UnitPrice").cast("double"))
    .withColumn("CustomerID", F.coalesce(F.col("CustomerID").cast("string"), F.lit("GUEST")))
    .withColumn("Country", F.trim(F.col("Country")))
    .withColumn("Description", F.trim(F.col("Description")))
    .withColumn("is_cancellation", F.upper(F.col("InvoiceNo")).startswith("C"))
    .withColumn("amount", F.round(F.col("Quantity") * F.col("UnitPrice"), 2))
    
    # FIX: Wrap the format pattern in F.lit() to protect it from the SQL analyzer
    .withColumn("InvoiceDate", F.try_to_timestamp(F.col("InvoiceDate"), F.lit("M/d/yyyy H:mm")))
    
    .withColumn(
        "is_administrative_fee", 
        F.upper(F.col("StockCode")).isin(["POST", "PADS", "M", "DOT", "BANK CHARGES", "CRUK", "D"])
    )
    .filter(
        F.col("Description").isNotNull() & 
        (F.col("Description") != "") &
        (F.col("UnitPrice") >= 0)
    )
    .withColumn("updated_at", F.current_timestamp())
)

# 3. Define the Upsert (Merge) logic for each micro-batch
def upsert_to_silver(micro_batch_df, batch_id):
    if micro_batch_df.isEmpty():
        return

    # PERFORMANCE FIX: Replacing global cluster .sort() with localized Window partitioning.
    # This prevents expensive full-cluster data shuffles on large batches.
    window_spec = Window.partitionBy("InvoiceNo", "StockCode", "CustomerID").orderBy(F.col("ingested_at").desc())
    
    deduplicated_batch = micro_batch_df \
        .withColumn("row_num", F.row_number().over(window_spec)) \
        .filter(F.col("row_num") == 1) \
        .drop("row_num")
    
    # Check if target silver table exists
    if not spark.catalog.tableExists(silver_table_name):
        deduplicated_batch.write \
            .format("delta") \
            .mode("append") \
            .saveAsTable(silver_table_name)
    else:
        target_table = DeltaTable.forName(spark, silver_table_name)
        
        # Safe Null Handling for CustomerID (Crucial for guest checkouts in UCI Retail)
        merge_condition = """
            target.InvoiceNo = source.InvoiceNo AND 
            target.StockCode = source.StockCode AND 
            target.CustomerID = source.CustomerID
        """
        
        target_table.alias("target") \
            .merge(
                source = deduplicated_batch.alias("source"),
                condition = merge_condition
            ) \
            .whenMatchedUpdateAll() \
            .whenNotMatchedInsertAll() \
            .execute()

# 4. Trigger the stream execution
query = silver_transformed_df.writeStream \
    .format("delta") \
    .foreachBatch(upsert_to_silver) \
    .option("checkpointLocation", silver_checkpoint_path) \
    .trigger(availableNow=True) \
    .start()

query.awaitTermination()
print(f"Silver layer update complete: Incremental data cleaned and merged into {silver_table_name}.")