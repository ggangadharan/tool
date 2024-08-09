from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, concat
from pyspark.sql.types import StringType, DoubleType
import argparse

# Parse command-line arguments
parser = argparse.ArgumentParser(description="Generate skewed data for customers and orders.")
parser.add_argument("--num_customers", type=int, default=10, help="Number of customers to generate.")
parser.add_argument("--target_duplicates", type=int, default=5059983792,
                    help="Target number of duplicate rows for skewing.")
parser.add_argument("--skew_factor", type=float, default=0.99, help="Factor to determine the skewness (0-1).")
parser.add_argument("--num_orders", type=int, default=500000000, help="Number of orders to generate.")
parser.add_argument("--num_partitions", type=int, default=200, help="Number of partitions for Spark operations.")
parser.add_argument("--customer_table", type=str, default="customers_spark", help="Output table name for customers.")
parser.add_argument("--order_table", type=str, default="orders_spark", help="Output table name for orders.")

args = parser.parse_args()

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Skewed Data Generation") \
    .getOrCreate()

# Configuration
num_customers = args.num_customers
target_duplicates = args.target_duplicates
skew_factor = args.skew_factor
num_orders = args.num_orders
num_partitions = args.num_partitions
customer_table = args.customer_table
order_table = args.order_table


# Generate skewed customer data
def generate_customer_data(spark, num_customers, target_duplicates, skew_factor, num_partitions):
    # Generate a base DataFrame with an appropriate number of rows
    num_rows = target_duplicates // num_partitions * num_partitions
    base_df = spark.range(num_rows).repartition(num_partitions)

    # Create a skewed customer ID column
    customer_df = base_df.withColumn(
        "CustomerID",
        when((col("id") % 100 < skew_factor * 100), lit(1))
        .otherwise((col("id") % (num_customers - 1)) + 2)
    ).drop("id")

    customer_df = customer_df.withColumn("Name", concat(lit("Customer "), col("CustomerID").cast(StringType())))
    customer_df = customer_df.withColumn("City", concat(lit("City "), col("CustomerID").cast(StringType())))

    return customer_df


# Generate skewed orders data
def generate_order_data(spark, num_customers, num_orders, num_partitions):
    order_df = spark.range(1, num_orders + 1).repartition(num_partitions)
    order_df = order_df.withColumn("CustomerID", (col("id") % num_customers) + 1)
    order_df = order_df.withColumn("Product", concat(lit("Product "), col("id").cast(StringType())))
    order_df = order_df.withColumn("Price", (col("id") % 90 + 10).cast(DoubleType()))

    return order_df


# Generate customer and order data
customer_df = generate_customer_data(spark, num_customers, target_duplicates, skew_factor, num_partitions)
order_df = generate_order_data(spark, num_customers, num_orders, num_partitions)

# Save DataFrames as tables
customer_df.write.mode('overwrite').saveAsTable(customer_table)
order_df.write.mode('overwrite').saveAsTable(order_table)

# Stop Spark session
spark.stop()
