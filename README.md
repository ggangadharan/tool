# Utilities 

## Title : skewed_data_generator.py

#### Description 

This script generates datasets with a specified level of skewness, useful for testing, modeling, and simulating real-world data scenarios where data distributions are not uniform or normal. It allows users to create custom datasets with controlled skewness, enabling robust evaluation of algorithms and models under various data conditions. The script can handle different types of distributions and provides flexibility in defining the degree of skewness to meet specific needs

It creates two tables customers and orders table. 

#### Usage 

To run this script with custom configurations, you can now use the following command:

```commandline
/usr/bin/spark-submit skewed_data_generator.py --num_customers 20 --skew_factor 0.95 --target_duplicates 1000000 --num_orders 1000000 --customer_table "my_customers" --order_table "my_orders"
```