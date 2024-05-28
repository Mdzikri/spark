from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
import os
from dotenv import load_dotenv
from pathlib import Path
from pyspark.sql import SparkSession

# Function to run the Spark job
def run_spark_job():
    import pyspark
    from pyspark.sql import SparkSession

    # Load environment variables
    dotenv_path = Path('/resources/.env')
    load_dotenv(dotenv_path=dotenv_path)

    postgres_host = os.getenv('POSTGRES_CONTAINER_NAME')
    postgres_dw_db = os.getenv('POSTGRES_DW_DB')
    postgres_user = os.getenv('POSTGRES_USER')
    postgres_password = os.getenv('POSTGRES_PASSWORD')

    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("Dibimbing") \
        .master('local') \
        .config("spark.jars", "/opt/bitnami/spark/jars/postgresql-42.2.18.jar") \
        .getOrCreate()

    # Define schema for purchases dataset
    purchases_schema = (
        "order_id INT, customer_id INT, product_id INT, quantity INT, price FLOAT"
    )

    # Create purchases dataframe
    purchases_data = [
        (101, 1, 1, 2, 19.99),
        (102, 2, 2, 1, 9.99),
        (103, 3, 3, 1, 15.99),
        (104, 1, 4, 1, 5.99),
        (105, 2, 5, 3, 12.99),
        (106, 3, 6, 2, 9.99),
        (107, 4, 7, 1, 11.99),
        (108, 1, 8, 2, 14.99),
        (109, 2, 9, 1, 9.99),
        (110, 3, 10, 1, 19.99),
    ]
    purchases_df = spark.createDataFrame(purchases_data, schema=purchases_schema)

    # Define schema for customers dataset
    customers_schema = "customer_id INT, name STRING, email STRING"

    # Create customers dataframe
    customers_data = [
        (1, "John Doe", "johndoe@example.com"),
        (2, "Jane Smith", "janesmith@example.com"),
        (3, "Bob Johnson", "bobjohnson@example.com"),
        (4, "Sue Lee", "suelee@example.com"),
    ]
    customers_df = spark.createDataFrame(customers_data, schema=customers_schema)

    # Define schema for products dataset
    products_schema = "product_id INT, name STRING, price FLOAT"

    # Create products dataframe
    products_data = [
        (1, "Product A", 19.99),
        (2, "Product B", 9.99),
        (3, "Product C", 15.99),
        (4, "Product D", 5.99),
        (5, "Product E", 12.99),
        (6, "Product F", 9.99),
        (7, "Product G", 11.99),
        (8, "Product H", 14.99),
        (9, "Product I", 9.99),
        (10, "Product J", 19.99),
    ]
    products_df = spark.createDataFrame(products_data, schema=products_schema)

    # Set join preferences
    spark.conf.set("spark.sql.join.preferSortMergeJoin", "true")
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")

    # Perform sort merge join
    merged_df = purchases_df.join(customers_df, "customer_id").join(products_df, "product_id")

    # Select unique columns to avoid duplicates
    merged_df = merged_df.select(
        "order_id", 
        "customer_id", 
        "product_id", 
        "quantity", 
        purchases_df.price.alias("purchase_price"), 
        customers_df.name.alias("customer_name"), 
        "email", 
        products_df.name.alias("product_name"), 
        products_df.price.alias("product_price")
    )

    merged_df.show()

    # JDBC connection properties
    jdbc_url = f'jdbc:postgresql://{postgres_host}/{postgres_dw_db}'
    jdbc_properties = {
        'user': postgres_user,
        'password': postgres_password,
        'driver': 'org.postgresql.Driver',
        'stringtype': 'unspecified'
    }

    # Write data to PostgreSQL
    merged_df.write \
        .mode("overwrite") \
        .jdbc(url=jdbc_url, table='public.real5', properties=jdbc_properties)

    # Read data from PostgreSQL for verification
    df = spark.read.jdbc(url=jdbc_url, table='public.real5', properties=jdbc_properties)
    df.show()

# Define the default_args dictionary
default_args = {
    "owner": "dibimbing",
    "retry_delay": timedelta(minutes=5),
}

# Define the DAG
spark_dag = DAG(
    dag_id="test_cok",
    default_args=default_args,
    schedule_interval=None,
    dagrun_timeout=timedelta(minutes=60),
    description="Test for spark submit",
    start_date=days_ago(1),
)

# Define the PythonOperator
run_spark = PythonOperator(
    task_id='run_spark_job',
    python_callable=run_spark_job,
    dag=spark_dag,
)

# Set the task
run_spark
