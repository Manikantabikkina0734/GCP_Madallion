import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from google.cloud import storage
from concurrent.futures import ThreadPoolExecutor

# Initialize Spark
spark = SparkSession.builder.appName("loadtosilver").getOrCreate()

# Load config file
with open("config.json", "r") as file:
    config = json.load(file)

# Get paths from config
silver_input_path = config["bronze"]["bronze_path"]  # gs://project1-gcp1/Bronze
silver_output_path = config["silver"]["silver_path"]  # gs://project1-gcp1/Silver
quarantine_path = f"{silver_output_path}/quarantine"

# Initialize GCS client
storage_client = storage.Client()
bucket_name = silver_input_path.split('/')[2]  # project1-gcp1
bucket = storage_client.bucket(bucket_name)

# Check unprocessed folders
def get_unprocessed_folders(silver_input_path, silver_output_path, table_name):
    input_prefix = f"{silver_input_path.split(bucket_name + '/')[1]}/{table_name}/"
    output_prefix = f"{silver_output_path.split(bucket_name + '/')[1]}/{table_name}/"

    source_blobs = bucket.list_blobs(prefix=input_prefix)
    source_dates = sorted(set(blob.name.split('/')[2] for blob in source_blobs if '/' in blob.name))

    destination_blobs = bucket.list_blobs(prefix=output_prefix)
    destination_dates = sorted(set(blob.name.split('/')[2] for blob in destination_blobs if '/' in blob.name))

    return [item for item in source_dates if item not in destination_dates]

# Read data from unprocessed folders
def read_data_for_unprocessed_folders(silver_input_path, table_name, unprocessed_folders):
    df_combined = None
    for date_folder in unprocessed_folders:
        folder_path = f"{silver_input_path}/{table_name}/{date_folder}"
        df = spark.read.csv(f"{folder_path}/*.csv", header=True, inferSchema=True)
        df_combined = df if df_combined is None else df_combined.union(df)
        print(f"Loaded data for {table_name} on {date_folder}")
    return df_combined

# Clean data
def clean_data(df, key_column):
    if df is not None:
        df = df.dropDuplicates([key_column]).dropna(subset=[key_column])
    return df

# Quarantine invalid rows
def quarantine_invalid_rows(df, table_name, date_folder):
    invalid_rows = None
    if table_name == "raw_order_items":
        invalid_rows = df.filter(col("quantity") < 0)
    if invalid_rows and invalid_rows.count() > 0:
        quarantine_dir = f"{quarantine_path}/{table_name}/{date_folder}"
        invalid_rows.coalesce(1).write.mode("overwrite").csv(f"{quarantine_dir}/{table_name}_quarantine.csv", header=True)
        print(f"Quarantined {invalid_rows.count()} rows for {table_name} on {date_folder}")
        df = df.subtract(invalid_rows)
    return df

# Enforce referential integrity
def enforce_referential_integrity(df, table_name, df_customers=None, df_orders=None):
    if table_name == "raw_orders" and df_customers is not None:
        df = df.join(df_customers.select("customer_id"), "customer_id", "inner")
    elif table_name == "raw_order_items" and df_orders is not None:
        df = df.join(df_orders.select("order_id"), "order_id", "inner")
    return df

# Write to silver
def write_to_silver(df, table_name, date_folder):
    output_dir = f"{silver_output_path}/{table_name}/{date_folder}"
    df.coalesce(1).write.mode("overwrite").csv(f"{output_dir}/{table_name}.csv", header=True)
    print(f"Data written to: {output_dir}/{table_name}.csv")

# Process table
def process_table(table, key):
    unprocessed_folders = get_unprocessed_folders(silver_input_path, silver_output_path, table)
    if unprocessed_folders:
        df_raw = read_data_for_unprocessed_folders(silver_input_path, table, unprocessed_folders)
        df_raw = clean_data(df_raw, key)
        if table in ["raw_orders", "raw_order_items"]:
            df_raw = enforce_referential_integrity(df_raw, table, df_customers, df_orders)
        for folder in unprocessed_folders:
            df_raw = quarantine_invalid_rows(df_raw, table, folder)
            write_to_silver(df_raw, table, folder)
    else:
        print(f"No new data for {table}.")

# Tables and keys
tables = {
    "raw_customers": "customer_id",
    "raw_orders": "order_id",
    "raw_order_items": "order_item_id",
    "raw_products": "product_id"
}

# Load reference data
df_customers = spark.read.csv(f"{silver_input_path}/raw_customers/*/*.csv", header=True, inferSchema=True)
df_orders = spark.read.csv(f"{silver_input_path}/raw_orders/*/*.csv", header=True, inferSchema=True)

# Process tables in parallel
with ThreadPoolExecutor() as executor:
    executor.map(lambda item: process_table(*item), tables.items())

# Stop Spark
spark.stop()