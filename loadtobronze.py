import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import count, input_file_name, regexp_extract
from google.cloud import storage

# Initialize Spark
spark = SparkSession.builder.appName("loadtobronze").getOrCreate()

# Load config file (assumes config.json is in the working directory via --files)
with open("config.json", "r") as file:
    config = json.load(file)

# Get paths from config
bronze_input_path = config["paths"]["output_folder"]  # gs://project1-gcp1/
bronze_output_path = config["bronze"]["bronze_path"]  # gs://project1-gcp1/Bronze

# Initialize GCS client
storage_client = storage.Client()
bucket_name = bronze_input_path.split('/')[2]  # project1-gcp1
bucket = storage_client.bucket(bucket_name)

# Check for unprocessed folders
def get_unprocessed_folders(bronze_input_path, bronze_output_path, table_name):
    input_prefix = f"{bronze_input_path.split(bucket_name + '/')[1]}{table_name}/"
    output_prefix = f"{bronze_output_path.split(bucket_name + '/')[1]}/{table_name}/"

    # List source directories
    source_blobs = bucket.list_blobs(prefix=input_prefix)
    source_dates = sorted(set(blob.name.split('/')[1] for blob in source_blobs if '/' in blob.name))

    # List destination directories
    destination_blobs = bucket.list_blobs(prefix=output_prefix)
    destination_dates = sorted(set(blob.name.split('/')[2] for blob in destination_blobs if '/' in blob.name))

    # Find unprocessed folders
    unprocessed_folders = [item for item in source_dates if item not in destination_dates]
    if not unprocessed_folders:
        print(f"No unprocessed folders for {table_name}.")
    return unprocessed_folders

# Add timestamp based on file name
def add_timestamp_to_df(df):
    timestamp_pattern = r"_(\d{8}_\d{6})\.csv"
    return df.withColumn("timestamp", regexp_extract(input_file_name(), timestamp_pattern, 1))

# Validate schema
def validate_schema(df, expected_columns):
    actual_columns = df.columns
    missing_columns = [col for col in expected_columns if col not in actual_columns]
    extra_columns = [col for col in actual_columns if col not in expected_columns]
    if missing_columns:
        print(f"Warning: Missing columns: {missing_columns}")
    if extra_columns:
        print(f"Warning: Extra columns: {extra_columns}")
    if not missing_columns and not extra_columns:
        print("Schema validation successful.")

# Write data to bronze
def write_to_bronze(df, folder_name, date_folder):
    output_dir = f"{bronze_output_path}/{folder_name}/{date_folder}"
    df.coalesce(1).write.mode("overwrite").csv(f"{output_dir}/{folder_name}.csv", header=True)
    print(f"Data written to: {output_dir}/{folder_name}.csv")

# Process data for a table
def process_data_for_table(table_name, expected_columns):
    unprocessed_folders = get_unprocessed_folders(bronze_input_path, bronze_output_path, table_name)
    if not unprocessed_folders:
        return

    for folder in unprocessed_folders:
        df_raw = spark.read.csv(f"{bronze_input_path}/{table_name}/{folder}/*.csv", header=True, inferSchema=True)
        df_raw = add_timestamp_to_df(df_raw)
        validate_schema(df_raw, expected_columns)
        write_to_bronze(df_raw, table_name, folder)

# Expected columns
expected_columns_customers = ["customer_id", "full_name", "email", "address", "city", "state", "timestamp"]
expected_columns_orders = ["order_id", "order_date", "customer_id", "total_amount", "order_status", "timestamp"]
expected_columns_order_items = ["order_item_id", "order_id", "product_id", "quantity", "unit_price", "timestamp"]
expected_columns_products = ["product_id", "product_name", "category", "price", "timestamp"]

# Process tables
process_data_for_table("raw_customers", expected_columns_customers)
process_data_for_table("raw_orders", expected_columns_orders)
process_data_for_table("raw_order_items", expected_columns_order_items)
process_data_for_table("raw_products", expected_columns_products)

# Stop Spark
spark.stop()