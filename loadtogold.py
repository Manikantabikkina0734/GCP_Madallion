import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, count, to_date, date_format
from google.cloud import storage

# Initialize Spark
spark = SparkSession.builder.appName("CreateFactSales_CSV").getOrCreate()

# Load config file
with open("config.json", "r") as file:
    config = json.load(file)

silver_input_path = config["silver"]["silver_path"]  # e.g., gs://project1-gcp1/Silver
gold_output_path = config["gold"]["gold_path"]       # e.g., gs://project1-gcp1/Gold

tables = ["raw_orders", "raw_order_items", "raw_products", "raw_customers"]

# Initialize GCS client
storage_client = storage.Client()
bucket_name = silver_input_path.split('/')[2]
bucket = storage_client.bucket(bucket_name)

def get_expected_states(unprocessed_folders=None):
    # Fallback to config if no unprocessed folders or if reading fails
    if "expected_states" in config and config["expected_states"]:
        states = config["expected_states"]
        print(f"Using expected states from config: {states}")
        return sorted(set(states))

    # If unprocessed folders are provided, read states from raw_customers in those folders
    if unprocessed_folders:
        customers_paths = [f"{silver_input_path}/raw_customers/{folder}/*.csv" for folder in unprocessed_folders]
        try:
            df_customers = spark.read.csv(customers_paths, header=True, inferSchema=True)
            states = [row["state"] for row in df_customers.select("state").distinct().collect() if row["state"] is not None and row["state"] != ""]
            print(f"Found states from raw_customers: {states}")
            return sorted(set(states)) if states else ["Unknown"]  # Fallback if no states found
        except Exception as e:
            print(f"Error reading raw_customers to get states: {e}")
            return ["Unknown"]  # Fallback to avoid failure
    else:
        print("No unprocessed folders provided, using default states ['CA', 'FL', 'GA']")
        return ["CA", "FL", "GA"]  # Default fallback states

def get_unprocessed_folders():
    # Get all source dates from one reference table (e.g., raw_orders)
    reference_table = "raw_orders"
    input_prefix = f"{silver_input_path.split(bucket_name + '/')[1]}/{reference_table}/"
    source_blobs = bucket.list_blobs(prefix=input_prefix)
    source_dates = sorted(set(blob.name.split('/')[2] for blob in source_blobs if '/' in blob.name))
    print(f"Source dates from {reference_table}: {source_dates}")

    # Get all processed dates from Gold/fact_sales across all states
    output_prefix = f"{gold_output_path.split(bucket_name + '/')[1]}/fact_sales/"
    destination_blobs = bucket.list_blobs(prefix=output_prefix)
    processed_dates_by_state = {}
    expected_states = get_expected_states()  # Get initial states (might be updated later)
    print(f"Initial expected states: {expected_states}")

    # Group processed dates by state
    for blob in destination_blobs:
        parts = blob.name.split('/')
        if len(parts) > 3:  # Structure: fact_sales/<state>/<date>
            state, date = parts[2], parts[3]
            if state not in processed_dates_by_state:
                processed_dates_by_state[state] = set()
            processed_dates_by_state[state].add(date)

    # Find unprocessed dates (those not processed for all expected states)
    unprocessed_folders = []
    for date in source_dates:
        processed_for_all_states = True
        for state in expected_states:
            if date not in processed_dates_by_state.get(state, set()):
                processed_for_all_states = False
                break
        if not processed_for_all_states:
            unprocessed_folders.append(date)

    print(f"Processed dates by state: {processed_dates_by_state}")
    print(f"Unprocessed folders: {unprocessed_folders}")

    # Update expected_states based on unprocessed folders if available
    if unprocessed_folders:
        expected_states = get_expected_states(unprocessed_folders)
        print(f"Updated expected states from unprocessed folders: {expected_states}")

    # Build table_unprocessed dict only if there are unprocessed folders
    table_unprocessed = {}
    if unprocessed_folders:
        for table in tables:
            table_unprocessed[table] = unprocessed_folders
    return table_unprocessed

def read_silver_data(table_name, unprocessed_folders):
    paths = [f"{silver_input_path}/{table_name}/{folder}/*.csv" for folder in unprocessed_folders]
    print(f"Reading data from paths: {paths}")
    df = spark.read.csv(paths, header=True, inferSchema=True)
    print(f"Read {df.count()} records from {table_name}")
    print(f"Schema of {table_name}:")
    df.printSchema()
    return df

def create_fact_sales():
    unprocessed_data = get_unprocessed_folders()
    if not unprocessed_data:
        print("No new data to process for fact_sales.")
        return

    all_unprocessed_folders = sorted(set(folder for folders in unprocessed_data.values() for folder in folders))
    print(f"All unprocessed folders: {all_unprocessed_folders}")

    for folder in all_unprocessed_folders:
        print(f"Processing data for folder: {folder}")

        df_orders = read_silver_data("raw_orders", [folder])
        df_order_items = read_silver_data("raw_order_items", [folder])
        df_products = read_silver_data("raw_products", [folder])
        df_customers = read_silver_data("raw_customers", [folder])

        if df_orders.isEmpty() or df_order_items.isEmpty() or df_products.isEmpty():
            print(f"Skipping folder {folder} due to missing data.")
            continue

        df_orders_filtered = df_orders.filter(col("order_status") != "CANCELLED")
        print(f"Orders after filtering: {df_orders_filtered.count()}")

        # Debug: Show sample of orders to check customer_id and order_status
        print("Sample of filtered orders:")
        df_orders_filtered.show(5)

        df_orders_filtered = df_orders_filtered.withColumn(
            "order_date",
            date_format(to_date(col("order_date")), "yyyyMMdd")
        )

        # Debug: Check customer_id types and values before join
        print("Distinct customer_ids in orders:")
        df_orders_filtered.select("customer_id").distinct().show(10)
        print("Schema of orders after filtering:")
        df_orders_filtered.printSchema()

        fact_sales = df_orders_filtered.alias("o") \
            .join(df_order_items.alias("oi"), col("o.order_id") == col("oi.order_id"), "inner") \
            .join(df_products.alias("p"), col("oi.product_id") == col("p.product_id"), "inner") \
            .join(df_customers.alias("c"), col("o.customer_id") == col("c.customer_id"), "left_outer") \
            .select(
                col("o.order_id"),
                col("o.customer_id"),
                col("c.full_name").alias("customer_name"),
                col("c.city").alias("customer_city"),
                col("c.state").alias("customer_state"),
                col("o.order_date"),
                col("o.order_status"),
                col("p.product_id"),
                col("p.product_name"),
                col("oi.quantity"),
                col("oi.unit_price"),
                (col("oi.quantity") * col("oi.unit_price")).alias("total_sales")
            )

        # Debug: Check the join result before aggregation
        print("Sample of fact_sales before aggregation:")
        fact_sales.show(5)
        print("Distinct customer_state values before aggregation:")
        fact_sales.select("customer_state").distinct().show()

        null_state_count = fact_sales.filter(col("customer_state").isNull() | (col("customer_state") == "")).count()
        print(f"Number of records with null/empty customer_state: {null_state_count}")

        fact_sales = fact_sales.groupBy(
            "order_id", "customer_id", "customer_name", "customer_city", "customer_state",
            "order_date", "order_status"
        ).agg(
            spark_sum("total_sales").alias("order_total"),
            count("*").alias("order_count")
        )

        print(f"Fact Sales Count: {fact_sales.count()}")
        if fact_sales.isEmpty():
            print(f"No records to write for folder {folder}. Skipping.")
            continue

        print("Sample of fact_sales after aggregation:")
        fact_sales.show(5)
        print("Distinct customer_state values after aggregation:")
        fact_sales.select("customer_state").distinct().show()

        states = [row["customer_state"] for row in fact_sales.select("customer_state").distinct().collect()]
        for state in states:
            if state is None or state == "":
                print(f"Warning: Found None/empty value for customer_state. Writing to 'Unknown' folder.")
                state = "Unknown"
            state_fact_sales = fact_sales.filter(col("customer_state") == state if state != "Unknown" else (col("customer_state").isNull() | (col("customer_state") == "")))
            output_folder = f"{gold_output_path}/fact_sales/{state}/{folder}"
            print(f"Writing fact_sales to: {output_folder}")
            state_fact_sales.write.mode("overwrite").option("header", "true").csv(output_folder)
            print(f"Fact table for {state} written to {output_folder}.")

create_fact_sales()
spark.stop()