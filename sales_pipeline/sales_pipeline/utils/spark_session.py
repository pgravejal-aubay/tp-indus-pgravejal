from pyspark.sql import SparkSession

def get_spark_session(app_name="SalesPipeline"):
    return SparkSession.builder.appName(app_name).getOrCreate()

def create_schemas(spark, config):
    catalog = config['catalog']['name']
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{config['catalog']['schemas']['bronze']}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{config['catalog']['schemas']['silver']}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{config['catalog']['schemas']['gold']}")