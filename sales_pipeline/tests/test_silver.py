import pytest
from pyspark.sql.types import IntegerType, DateType
from sales_pipeline.silver.cleaning import clean_single_city_df

def test_clean_single_city_transformation(spark_session):

    data_sales = [
        ("1", "01/03/2024", "Product A", "Cat A", "10", 100.0, 10.0),
        ("2", "02/03/2024", "Product B", "Cat B", "5", 50.0, 10.0)
    ]
    cols_sales = ["ID_Sale", "Sale_Date", "Product_Name", "Category", "Quantity", "Total_Amount", "Unit_Price"]
    df_sales = spark_session.createDataFrame(data_sales, cols_sales)

    data_catalog = [("Product A", "Produit A", "Catégorie A")]
    cols_catalog = ["Nom_Produit_Anglais", "Nom_Produit_Francais", "Catégorie_Francais"]
    df_catalog = spark_session.createDataFrame(data_catalog, cols_catalog)

    fake_config = {
        "city_label": "Paris",
        "country": "France",
        "currency": "EUR"
    }

    df_result = clean_single_city_df(df_sales, df_catalog, fake_config, "paris")
    
    # Vérification des noms de colonnes
    expected_columns = ["ID_Vente", "Date_Vente", "Nom_Produit", "Catégorie", "Quantité", "Montant_Total", "Prix_Unitaire", "Ville", "Pays", "Devise", "Nom_Boutique"]
    assert sorted(df_result.columns) == sorted(expected_columns)

    # Vérification des types de données
    dtypes = dict(df_result.dtypes)
    assert dtypes["Quantité"] == "int"
    assert dtypes["Date_Vente"] == "date"

    # Vérification de la valeur
    row_prod_a = df_result.filter(df_result.ID_Vente == "1").first()
    assert row_prod_a["Nom_Produit"] == "Produit A"
    
    # Vérification de l'enrichissement
    assert row_prod_a["Ville"] == "Paris"
    assert row_prod_a["Devise"] == "EUR"