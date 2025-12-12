import pytest
from sales_pipeline.gold.aggregation import apply_currency_conversion

def test_currency_conversion_usd(spark_session):
    data = [
        ("Produit A", 100.0, "USD"), 
        ("Produit B", 100.0, "EUR")
    ]
    columns = ["Nom_Produit", "Montant_Total", "Devise"]
    df_input = spark_session.createDataFrame(data, columns)

    rates = {"USD": 0.85, "JPY": 0.01}

    df_result = apply_currency_conversion(df_input, rates)

    rows = df_result.collect()

    row_usd = [r for r in rows if r["Devise"] == "USD"][0]
    assert row_usd["Montant_Total"] == 85.0

    row_eur = [r for r in rows if r["Devise"] == "EUR"][0]
    assert row_eur["Montant_Total"] == 100.0