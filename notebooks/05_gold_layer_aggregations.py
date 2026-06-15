from pyspark.sql import functions as F

# 1. Configuration
silver_table_name = "workspace.default.silver_sales"

gold_weekly_revenue_table = "workspace.default.gold_weekly_revenue"
gold_top_products_table = "workspace.default.gold_top_products"
gold_top_customers_table = "workspace.default.gold_top_customers"
gold_country_revenue_table = "workspace.default.gold_country_revenue"

gold_checkpoint_base = "/Volumes/workspace/default/project/checkpoints/gold_"

# 2. Read incrementally from the Silver Table
silver_stream = spark.readStream.table(silver_table_name)

# 3. Define Gold Aggregations & Streams

# Query A: Weekly Revenue (Utilizing Watermarking for streaming stability)
query_weekly = (
    silver_stream
    .withWatermark("InvoiceDate", "10 days")
    .groupBy(F.window("InvoiceDate", "1 week"))
    .agg(F.round(F.sum("amount"), 2).alias("weekly_revenue"))
    .select(
        F.col("window.start").alias("week_start"),
        F.col("window.end").alias("week_end"),
        "weekly_revenue"
    )
    .writeStream
    .format("delta")
    .outputMode("complete")
    .option("checkpointLocation", f"{gold_checkpoint_base}weekly")
    .trigger(availableNow=True)
    .toTable(gold_weekly_revenue_table)
)

# Query B: Top Products by Revenue (Filtering out administrative overhead items)
query_products = (
    silver_stream
    .filter(F.col("is_administrative_fee") == False)
    .groupBy("StockCode", "Description")
    .agg(F.round(F.sum("amount"), 2).alias("total_revenue"))
    .writeStream
    .format("delta")
    .outputMode("complete")
    .option("checkpointLocation", f"{gold_checkpoint_base}products")
    .trigger(availableNow=True)
    .toTable(gold_top_products_table)
)

# Query C: Top Customers (Excluding generic guest checkouts)
query_customers = (
    silver_stream
    .filter(F.col("CustomerID") != "GUEST_CHECKOUT")
    .groupBy("CustomerID")
    .agg(F.round(F.sum("amount"), 2).alias("total_spent"))
    .writeStream
    .format("delta")
    .outputMode("complete")
    .option("checkpointLocation", f"{gold_checkpoint_base}customers")
    .trigger(availableNow=True)
    .toTable(gold_top_customers_table)
)

# Query D: Total Revenue per Country
query_country = (
    silver_stream
    .groupBy("Country")
    .agg(F.round(F.sum("amount"), 2).alias("total_revenue"))
    .writeStream
    .format("delta")
    .outputMode("complete")
    .option("checkpointLocation", f"{gold_checkpoint_base}country")
    .trigger(availableNow=True)
    .toTable(gold_country_revenue_table)
)

# 4. Await Termination for all parallel streaming jobs
print("Processing incremental updates for all Gold tables...")
query_weekly.awaitTermination()
query_products.awaitTermination()
query_customers.awaitTermination()
query_country.awaitTermination()

print("Gold layer business aggregations successfully updated!")