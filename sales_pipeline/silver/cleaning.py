from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import IntegerType, DoubleType
from pyspark.sql.functions import col, lit, when, to_date, monotonically_increasing_id

def clean_single_city_df(df_city: DataFrame, df_catalog: DataFrame, city_config: dict, city_name: str) -> DataFrame:
    df = df_city.withColumnRenamed("ID_Sale", "ID_Vente") \
        .withColumnRenamed("Sale_Date", "Date_Vente")\
        .withColumn("Date_Vente", to_date(col("Date_Vente"), "dd/MM/yyyy"))\
        .withColumnRenamed("Product_Name", "Nom_Produit") \
        .withColumnRenamed("Category", "Catégorie") \
        .withColumnRenamed("Quantity", "Quantité")\
        .withColumn("Quantité", col("Quantité").cast(IntegerType()))\
        .withColumnRenamed("Total_Amount","Montant_Total") \
        .withColumnRenamed("Unit_Price", "Prix_Unitaire")

    df = df.join(df_catalog, df.Nom_Produit == df_catalog.Nom_Produit_Anglais, "left") \
        .withColumn("Nom_Produit", when(col("Nom_Produit_Francais").isNotNull(), col("Nom_Produit_Francais")).otherwise(col("Nom_Produit"))) \
        .withColumn("Catégorie", when(col("Catégorie_Francais").isNotNull(), col("Catégorie_Francais")).otherwise(col("Catégorie"))) \
        .withColumn("Quantité", col("Quantité").cast(IntegerType())) \
        .select("ID_Vente", "Date_Vente", "Nom_Produit", "Catégorie", "Quantité", "Montant_Total", "Prix_Unitaire")

    df = df.withColumn("Ville", lit(city_config.get("city_label", city_name))) \
           .withColumn("Pays", lit(city_config['country'])) \
           .withColumn("Devise", lit(city_config['currency'])) \
           .withColumn("Nom_Boutique", lit(f"boutique_{city_name}"))
           
    return df

def run_silver_transformation(spark, config):
    bronze_schema = f"{config['catalog']['name']}.{config['catalog']['schemas']['bronze']}"
    silver_dst = f"{config['catalog']['name']}.{config['catalog']['schemas']['silver']}.all_boutique"

    df_catalog = spark.table(f"{bronze_schema}.catalogue_produits")

    all_boutique = None

    for city_name, city_conf in config['cities'].items():
        table_name = f"{bronze_schema}.{city_conf['table_name']}"
        df_city = spark.table(table_name)

        df_clean = clean_single_city_df(df_city, df_catalog, city_conf, city_name)

        if all_boutique is None:
            all_boutique = df_clean
        else:
            all_boutique = all_boutique.unionByName(df_clean)

    if all_boutique:
        all_boutique = all_boutique.withColumn("ID_Vente", monotonically_increasing_id())
        
        print(f"Writing Silver table to {silver_dst}")
        all_boutique.write.mode("overwrite").saveAsTable(silver_dst)