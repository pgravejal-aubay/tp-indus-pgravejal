# Databricks notebook source
# MAGIC %pip install -e ".[dev]"

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import pytest
import sys
import os

sys.dont_write_bytecode = True 

args = [
    "tests/",
    "-v",
    "-p", "no:cacheprovider",
    "-p", "no:warnings"
]

# Lancement
retcode = pytest.main(args)

if retcode == 0:
    print("\n TOUS LES TESTS SONT PASSÉS !")
else:
    print(f"\n ECHEC : Code de retour {retcode}")
    raise Exception("Les tests unitaires ont échoué.")