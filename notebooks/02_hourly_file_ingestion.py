import os

# Define your paths
archives_path = "/Volumes/workspace/default/project/archives"
landing_path = "/Volumes/workspace/default/project/landing"
log_path = f"{archives_path}/.processed_log.txt"

# 1. Get all files in the archives folder
all_files = dbutils.fs.ls(archives_path)

# Filter for your specific sales files (sales_YYYY_MM.csv)
sales_files = [f.name for f in all_files if f.name.startswith("sales_") and f.name.endswith(".csv")]

# 2. Sort them chronologically
# Because of the sales_YYYY_MM.csv format, alphabetical sorting matches chronological sorting!
sales_files.sort()

# 3. Read the log of already processed files
processed_files = set()
try:
    # Check if the log file exists by trying to read it
    log_content = dbutils.fs.head(log_path)
    processed_files = set(log_content.strip().split("\n"))
except Exception:
    # If file doesn't exist, we start fresh
    pass

# 4. Find the oldest file that hasn't been moved yet
file_to_move = None
for f_name in sales_files:
    if f_name not in processed_files:
        file_to_move = f_name
        break

# 5. Move the file and update the log
if file_to_move:
    source = f"{archives_path}/{file_to_move}"
    destination = f"{landing_path}/{file_to_move}"
    
    # Copy file to landing, then delete from archives (effectively a move)
    dbutils.fs.cp(source, destination)
    dbutils.fs.rm(source)
    
    # Append the moved file to our tracking log
    processed_files.add(file_to_move)
    new_log_data = "\n".join(processed_files)
    dbutils.fs.put(log_path, new_log_data, overwrite=True)
    
    print(f"Successfully moved: {file_to_move} -> {landing_path}")
else:
    print("No new files found to move. All archives have been processed.")