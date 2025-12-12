from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit, when, sum, round, month, year

def apply_currency_conversion(df, rates):
    """Convertit tout en EUR selon les taux de la config"""
    return df.withColumn("Montant_Total",
        when(col("Devise") == "USD", col("Montant_Total") * lit(rates['USD']))
        .when(col("Devise") == "JPY", col("Montant_Total") * lit(rates['JPY']))
        .otherwise(col("Montant_Total"))
    )

def run_gold_aggregations(spark, config):
    silver_src = f"{config['catalog']['name']}.{config['catalog']['schemas']['silver']}.all_boutique"
    gold_schema = f"{config['catalog']['name']}.{config['catalog']['schemas']['gold']}"
    
    df_silver = spark.table(silver_src)

    df_converted = apply_currency_conversion(df_silver, config['exchange_rates'])
    
    # KPI 1: CA Global par mois
    df_ca_global = df_converted.groupBy(month("Date_Vente").alias("mois"), year("Date_Vente").alias("annee")) \
        .agg(sum("Montant_Total").alias("CA")) \
        .orderBy("mois") \
        .withColumn("CA", round(col("CA"), 2))
    
    df_ca_global.write.mode("overwrite").saveAsTable(f"{gold_schema}.chiffre_affaire")
    
    # KPI 2: CA par Boutique
    df_ca_boutique = df_converted.groupBy(month("Date_Vente").alias("mois"), year("Date_Vente").alias("annee"), "Nom_Boutique") \
        .agg(sum("Montant_Total").alias("CA")) \
        .orderBy("mois") \
        .withColumn("CA", round(col("CA"), 2))
        
    df_ca_boutique.write.mode("overwrite").saveAsTable(f"{gold_schema}.chiffre_affaire_boutique")

    # KPI 3: Produits les plus vendus (Quantité)
    df_top_qty = df_converted.groupBy("Nom_Produit") \
        .agg(sum("Quantité").alias("Quantité")) \
        .orderBy(col("Quantité").desc())
        
    df_top_qty.write.mode("overwrite").saveAsTable(f"{gold_schema}.produits_plus_vendus")

    # KPI 4: Produits les plus rentables (CA)
    df_top_ca = df_converted.groupBy("Nom_Produit") \
        .agg(sum("Montant_Total").alias("CA")) \
        .orderBy(col("CA").desc()) \
        .withColumn("CA", round(col("CA"), 2))
        
    df_top_ca.write.mode("overwrite").saveAsTable(f"{gold_schema}.produits_plus_ca")
    
    print("Gold aggregations finished.")