# 🛍️ Sales Pipeline - Databricks Edition
Ce projet implémente un pipeline de données ELT (Extract, Load, Transform) suivant l'architecture "Medallion" (Bronze, Silver, Gold). Il a pour but de traiter les données de ventes internationales (Paris, New York, Tokyo), de les normaliser et de calculer des KPIs business.
Ce code est conçu pour être modulaire, testable et déployable sur Databricks ou tout environnement Spark.
## 🎯 But du projet
L'objectif est de transformer des fichiers bruts (CSV) déposés dans un volume en tableaux de bord analytiques exploitables.
**Bronze (Ingestion) :** Chargement des données brutes (CSV) vers des tables Delta, avec archivage des fichiers sources.
**Silver (Cleaning) :** Nettoyage, typage, dédoublonnage, traduction des produits et enrichissement (Pays, Devise).
**Gold (Aggregation) :** Conversion des devises en Euro et calcul des KPIs (CA par mois, Top Produits, etc.).

## 🛠️ Installation et Prérequis
### 1. Environnement Local
Pour développer ou tester en local, vous avez besoin de Python (>=3.9) et de Java (pour Spark).
Installez les dépendances et l'environnement de développement :

`pip install -e .[dev]`
### 2. Sur Databricks
Le projet est conçu pour être exécuté via Databricks Repos (Git Folders) ou packagé sous forme de fichier .whl.
L'environnement Databricks fournit nativement Spark.
## 🚀 Comment exécuter le pipeline
### En local (pour le développement)
Le point d'entrée unique est le fichier main.py. Il orchestre les trois étapes (Bronze -> Silver -> Gold).

`python main.py`
### Sur Databricks (Production)
Créer un Job Databricks.
Configurer la tâche pour utiliser le script main.py (via Git Source).
Assurez-vous que le cluster a accès au volume de données défini dans config/config.yaml.
## 🧪 Lancer les tests
Les tests unitaires garantissent que la logique de transformation (Silver/Gold) ne régresse pas. Ils utilisent une session Spark locale.
`pytest tests/`
