# Databricks notebook source
import os
import pandas as pd

# Définissez les chemins des répertoires "raw" et "processed"
raw_directory = "wasbs://data@aminbenstorage.blob.core.windows.net/public_transport_data/raw/"
processed_directory = "wasbs://data@aminbenstorage.blob.core.windows.net/public_transport_data/processed/"

# Obtenez la liste des fichiers CSV dans le répertoire "raw"
for mois in mois:
    raw_month_directory = os.path.join(raw_directory, mois.capitalize())
    files = [f for f in os.listdir(raw_month_directory) if f.endswith(".csv")]

    for file in files:
        # Lisez le fichier CSV
        csv_path = os.path.join(raw_month_directory, file)
        df = pd.read_csv(csv_path)

        # Effectuez des opérations de nettoyage sur le DataFrame 'df'
        # Par exemple, vous pouvez supprimer les lignes avec des données incorrectes ou manquantes

        # Enregistrez le DataFrame nettoyé dans le répertoire "processed"
        processed_month_directory = os.path.join(processed_directory, mois.capitalize())
        if not os.path.exists(processed_month_directory):
            os.makedirs(processed_month_directory)

        processed_csv_path = os.path.join(processed_month_directory, file)
        df.to_csv(processed_csv_path, index=False)

