import os
import shutil
from pyspark.sql import SparkSession

def ingest_product_catalog(spark: SparkSession, config: dict):
    """Ingestion du catalogue produit unique"""
    base_path = config['paths']['base_volume']
    catalog_path = f"{base_path}/{config['paths']['product_catalog']}"
    dest_table = f"{config['catalog']['name']}.{config['catalog']['schemas']['bronze']}.catalogue_produits"

    print(f"Ingesting Product Catalog to {dest_table}...")
    df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(catalog_path)
    df.write.mode("overwrite").saveAsTable(dest_table)

def ingest_sales_data(spark: SparkSession, config: dict):
    """Ingestion des ventes par ville et archivage"""
    base_path = config['paths']['base_volume']
    bronze_schema = f"{config['catalog']['name']}.{config['catalog']['schemas']['bronze']}"

    for city_name, city_conf in config['cities'].items():
        table_name = f"{bronze_schema}.{city_conf['table_name']}"
        city_path = f"{base_path}/boutique_{city_name}"
        
        files_to_process = [
            (f"{city_path}/{city_name}_sales_march_2024.csv", "overwrite"),
            (f"{city_path}/{city_name}_sales_april_2024.csv", "append")
        ]

        print(f"Processing city: {city_name} into {table_name}")

        archive_dir = f"{city_path}/archived"
        os.makedirs(archive_dir, exist_ok=True)

        for file_path, write_mode in files_to_process:
            if os.path.exists(file_path):
                print(f"Reading {file_path}")
                df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(file_path)
                df.write.mode(write_mode).saveAsTable(table_name)
                
                # Archivage
                print(f"Archiving {file_path}")
                shutil.move(file_path, archive_dir)
            else:
                print(f"Warning: File {file_path} not found.")