# Databricks notebook source
#Connection configuration
spark.conf.set(
"fs.azure.account.key.aminbenstorage.blob.core.windows.net", "B2vg1vuvYtkcygLcTLXhErl9DccZRYrGkrtXROsTvfIes2c/QM4vfyFfJdTSXv0riqXi/0iiNucV+ASt0IgRgw=="
)

# COMMAND ----------

from pyspark.sql.functions import avg, count, year, month, dayofmonth, dayofweek, to_date, col, expr, unix_timestamp, when

# Fonction pour traiter un fichier CSV
def process_csv(input_df):
    # Convertir la colonne "Date" en un format de date
    input_df = input_df.withColumn("Date", to_date("Date", "yyyy-MM-dd"))

    # Extraire l'année, le mois, le jour et le jour de la semaine
    input_df = input_df.withColumn("Year", year("Date"))
    input_df = input_df.withColumn("Month", month("Date"))
    input_df = input_df.withColumn("Day", dayofmonth("Date"))
    input_df = input_df.withColumn("DayOfWeek", dayofweek("Date"))

    # Supprimer les lignes où les deux premiers caractères de la colonne "ArrivalTime" sont "24" ou supérieurs à "24"
    input_df = input_df.filter(~(col("ArrivalTime").substr(1, 2) >= "24"))
    input_df = input_df.filter(~(col("DepartureTime").substr(1, 2) >= "24"))

    # Calculer la colonne "Duration"
    input_df = input_df.withColumn("Duration", expr(
        "from_unixtime(unix_timestamp(ArrivalTime, 'HH:mm') - unix_timestamp(DepartureTime, 'HH:mm'), 'HH:mm')"
    ))

    # Catégoriser les retards en fonction de la colonne "Delay"
    input_df = input_df.withColumn("DelayCategory", 
                   when(col("Delay") <= 0, "No Delay")
                   .when((col("Delay") > 0) & (col("Delay") <= 10), "Short Delay")
                   .when((col("Delay") > 10) & (col("Delay") <= 20), "Medium Delay")
                   .otherwise("Long Delay"))

    # Identifier les heures de pointe et heures hors pointe en fonction du nombre de passagers
    average_passengers = input_df.select(avg("Passengers")).first()[0]
    input_df = input_df.withColumn("HeureDePointe", when(col("Passengers") > average_passengers, "peak").otherwise("off-peak"))

    return input_df

# Fonction pour agréger les données
def aggregate_data(input_df):
    result_df = input_df.groupBy("Route").agg(
        avg("Delay").alias("RetardMoyen"),
        avg("Passengers").alias("NombrePassagersMoyen"),
        count("*").alias("NombreTotalVoyages")
    )
    return result_df

# COMMAND ----------

# Lecture du fichier CSV
spark_df = spark.read.format('csv').option('header', True).load("wasbs://data@aminbenstorage.blob.core.windows.net/public_transport_data/raw/public-transport-data.csv")

# Appliquer la première fonction pour effectuer le prétraitement
processed_df = process_csv(spark_df)

# Appliquer la deuxième fonction pour agréger les données
aggregated_df = aggregate_data(processed_df)

# Afficher les résultats
display(aggregated_df)

# COMMAND ----------


