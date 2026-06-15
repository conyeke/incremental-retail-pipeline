# 1. Install and Fetch Dataset (Pandas-based)
# %pip install ucimlrepo  # Run this in a separate cell if needed
from ucimlrepo import fetch_ucirepo
from pyspark.sql import functions as F
import pandas as pd

# fetch dataset
online_retail = fetch_ucirepo(id=352)

# data (as pandas dataframes) 
x = online_retail.data.ids
y = online_retail.data.features

# Combine ids and features horizontally
df = pd.concat([x, y], axis=1)

# 2. BRIDGE THE GAP: Convert Pandas DataFrame to PySpark DataFrame
# (Note: Databricks standard clusters have spark session pre-defined as 'spark')
df_spark = spark.createDataFrame(df)

# 3. Prepare: Explicitly provide the date format to handle '12/1/2010 8:26'
df_prep = df_spark.withColumn("year", F.year(F.to_date("InvoiceDate", "M/d/yyyy H:m"))) \
                  .withColumn("month", F.month(F.to_date("InvoiceDate", "M/d/yyyy H:m")))

# 4. Get unique combinations for iteration
periods = df_prep.select("year", "month").dropna().distinct().collect()

# 5. Define the base output directory
base_path = "/Volumes/workspace/default/project/archives"

for row in periods:
    yr, mo = row['year'], row['month']
    
    # Filter for this specific month/year
    temp_df = df_prep.filter((F.col("year") == yr) & (F.col("month") == mo))
    
    # Define the target filenames and paths
    file_name = f"sales_{yr}_{mo}.csv"
    temp_path = f"{base_path}/temp_{yr}_{mo}"
    final_path = f"{base_path}/{file_name}"
    
    # 6. Write to a temporary folder as a single CSV
    temp_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(temp_path)
    
    # 7. Move the actual CSV out of the folder and rename it
    files = dbutils.fs.ls(temp_path)
    csv_file = [f.path for f in files if f.path.endswith(".csv")][0]
    
    dbutils.fs.cp(csv_file, final_path)
    
    # Clean up the temporary folder
    dbutils.fs.rm(temp_path, recurse=True)
    
    print(f"Archived: {final_path}")