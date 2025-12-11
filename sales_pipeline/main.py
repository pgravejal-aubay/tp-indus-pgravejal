import yaml
from sales_pipeline.utils.spark_session import get_spark_session, create_schemas
from sales_pipeline.bronze.ingestion import ingest_sales_data, ingest_product_catalog
from sales_pipeline.silver.cleaning import run_silver_transformation
from sales_pipeline.gold.aggregation import run_gold_aggregations

def load_config(path="sales_pipeline/config/config.yaml"):
    with open(path, 'r') as f:
        return yaml.safe_load(f)

def main():
    spark = get_spark_session()
    config = load_config()
    
    print("Pipeline starts")
    create_schemas(spark, config)
    
    # 2. Bronze Layer
    ingest_product_catalog(spark, config)
    ingest_sales_data(spark, config)
    
    # 3. Silver Layer
    run_silver_transformation(spark, config)
    
    # 4. Gold Layer
    run_gold_aggregations(spark, config)
    
    print("Pipeline Execution Successful!")

if __name__ == "__main__":
    main()