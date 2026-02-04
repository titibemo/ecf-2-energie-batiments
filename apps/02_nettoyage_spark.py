import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
builder: SparkSession.Builder = SparkSession.builder
from pyspark.sql.functions import udf, col, lit, min, max, avg, desc, round, count, when
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, DoubleType, TimestampType
from datetime import datetime

# 1. CONFIGURATION ENVIRONNEMENT
os.environ['HADOOP_HOME'] = "C:\\hadoop"
os.environ['PATH'] = os.environ['PATH'] + ";" + "C:\\hadoop\\bin"

def create_spark_session():
    """Cree et configure la session Spark."""
    spark = SparkSession.builder \
        .appName("ECF 2 - Nettoyage") \
        .master("local[*]") \
        .config("spark.driver.memory", "4g") \
        .config("spark.sql.shuffle.partitions", "8") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    return spark

def parse_multi_format_timestamp(timestamp_str):
    """
    UDF pour parser les timestamps multi-formats.
    Formats supportes:
    - %Y-%m-%d %H:%M:%S (ISO)
    - %d/%m/%Y %H:%M (FR)
    - %m/%d/%Y %H:%M:%S (US)
    - %Y-%m-%dT%H:%M:%S (ISO avec T)
    """
    if timestamp_str is None:
        return None

    formats = [
        "%Y-%m-%dT%H:%M:%S",  # ISO avec T
        "%Y-%m-%d %H:%M:%S",  # ISO avec espace
        "%d/%m/%Y %H:%M:%S",  # français avec secondes
        "%d/%m/%Y %H:%M",      # français sans secondes
        "%m/%d/%Y %H:%M:%S"   # américain avec secondes
    ]

    for fmt in formats:
        try:
            return datetime.strptime(timestamp_str, fmt)
        except ValueError:
            continue

    return None

def clean_value(value_str):
    """
    UDF pour nettoyer les valeurs numeriques.
    - Remplace la virgule par un point
    - Retourne None pour les valeurs non numeriques
    """
    if value_str is None:
        return None

    try:
        # Remplacer virgule par point
        clean_str = value_str.replace(",", ".")
        return float(clean_str)
    except (ValueError, AttributeError):
        return None


def main():
    # session spark
    spark = create_spark_session()

    # chargement des donnees
    df_raw_consommations = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .option("sep", ",") \
        .csv("/data/consommations_raw.csv")

    # ligne en entrée avant netotyage
    initial_count = df_raw_consommations.count()
    print(f"  Lignes en entree: {initial_count:,}")

    # Enregistrer les UDFs
    parse_timestamp_udf = F.udf(parse_multi_format_timestamp, TimestampType())
    clean_value_udf = F.udf(clean_value, DoubleType())

    #################### TIMESTAMP

    # Parser les timestamps multi-formats
    print("\n[1/] Parsing des timestamps multi-formats...")
    df_consommations_with_correct_timestamp = df_raw_consommations.withColumn(
        "timestamp_parsed",
        parse_timestamp_udf(F.col("timestamp"))
    )

    # statistiques sur les timestamps invalides
    invalid_timestamps = df_consommations_with_correct_timestamp.filter(F.col("timestamp_parsed").isNull()).count()
    df_consommations_with_correct_timestamp = df_consommations_with_correct_timestamp.filter(F.col("timestamp_parsed").isNotNull())
    print(f"  Timestamps invalides supprimes: {invalid_timestamps:,}")

    #################### CONSOMMATION

     # Convertir les valeurs avec virgule decimale en float
    print("\n[2/] Conversion des valeurs numeriques...")
    df_consommations_with_correct_values = df_consommations_with_correct_timestamp.withColumn(
        "consommation_clean",
        clean_value_udf(F.col("consommation"))
    )

    invalid_values = df_consommations_with_correct_values.filter(F.col("consommation_clean").isNull()).count()
    df_consommations_with_correct_values = df_consommations_with_correct_values.filter(F.col("consommation_clean").isNotNull())
    print(f"  Valeurs non numeriques supprimees: {invalid_values:,}")


    # Supprimer les valeurs non numériques
    print("\n[3/] Suppression des valeurs non numeriques...")
    invalid_values = df_consommations_with_correct_values.filter(F.col("consommation_clean").isNull()).count()
    df_consommations_with_correct_values = df_consommations_with_correct_values.filter(F.col("consommation_clean").isNotNull())
    print(f"  Valeurs non numeriques supprimees: {invalid_values:,}")

    print("\n[4/6] Suppression des valeurs aberrantes...")
    negative_count = df_consommations_with_correct_values.filter(F.col("consommation_clean") < 0).count()
    outlier_count = df_consommations_with_correct_values.filter(F.col("consommation_clean") > 10000).count()

    df_clean = df_consommations_with_correct_values.filter(
        (F.col("consommation_clean") >= 0) & (F.col("consommation_clean") < 10000)
    )
    print(f"  Valeurs negatives supprimees: {negative_count:,}")
    print(f"  Outliers (>10000) supprimes: {outlier_count:,}")

    # Dedupliquer sur (batiment_id, timestamp, type_energie)
    before_dedup = df_clean.count()
    df_dedup = df_clean.dropDuplicates(["batiment_id", "timestamp_parsed", "type_energie"])
    after_dedup = df_dedup.count()
    duplicates_removed = before_dedup - after_dedup
    print(f"  Doublons supprimes: {duplicates_removed:,}")

    print("\n[5/6] Suppression des valeurs aberrantes...")
    # Calculer les agregations :


    # - Consommations horaires moyennes par batiment
    df_clean_with_time = df_dedup.withColumn(
        "date", F.to_date(F.col("timestamp_parsed"))
    ).withColumn(
        "hour", F.hour(F.col("timestamp_parsed"))
    ).withColumn(
        "year", F.year(F.col("timestamp_parsed"))
    ).withColumn(
        "month", F.month(F.col("timestamp_parsed"))
    )

    # - Consommations journalieres par batiment et type d'energie
    df_by_hour = df_clean_with_time.groupBy(
        "batiment_id", "type_energie", "date", "hour", "year", "month"
    ).agg(
        F.round(F.mean("consommation_clean"), 2).alias("consommation_mean"),
        F.round(F.min("consommation_clean"), 2).alias("consommation_min"),
        F.round(F.max("consommation_clean"), 2).alias("consommation_max"),
        F.count("*").alias("consommation_par_jour")
    )

    print(f"  Consommations journalieres par batiment et type d'energie: {df_by_hour.count():,}")

    # - Consommations mensuelles par batiment et type d'energie
    df_by_month = df_clean_with_time.groupBy(
        "batiment_id", "type_energie", "year", "month"
    ).agg(
        F.round(F.mean("consommation_clean"), 2).alias("consommation_mean"),
        F.round(F.min("consommation_clean"), 2).alias("consommation_min"),
        F.round(F.max("consommation_clean"), 2).alias("consommation_max"),
        F.count("*").alias("consommation_par_mois")
    )

    print(f"  Consommations mensuelles par batiment et type d'energie: {df_by_month.count():,}")



    # Consommations mensuelles par commune
    df_batiments_raw = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .option("sep", ",") \
        .csv("/data/batiments.csv")

    df_ref_communes = df_batiments_raw.select("batiment_id", "commune")

    # 2. On joint cette info au dataframe principal
    df_enriched = df_clean_with_time.join(
        df_ref_communes, 
        "batiment_id", 
        "left"
    )


    # - Consommations mensuelles par COMMUNE
    df_by_commune_month = df_enriched.groupBy(
        "commune", "type_energie", "year", "month"
    ).agg(
        F.round(F.sum("consommation_clean"), 2).alias("consommation_totale"),
        F.round(F.mean("consommation_clean"), 2).alias("consommation_moyenne"),
        F.countDistinct("batiment_id").alias("nb_batiments_actifs")
    )

    print(f"  Consommations mensuelles par commune: {df_by_commune_month.count():,}")

    df_enriched.show(5)

    print("\n[6/6] Suppression des valeurs aberrantes...")
    # Sauvegarder en Parquet partitionné par date.
    df_enriched.write \
        .mode("overwrite") \
        .partitionBy("date", "type_energie") \
        .parquet("/data/output/consommations_clean/")


    # sauvegarder les données en csv et renommé le fichier
    #df_clean_with_time.toPandas().to_csv("/data/consommation_clean.csv", index=False)
    # df_clean_with_time.coalesce(1).write \
    # .mode("overwrite") \
    # .option("header", "true") \
    # .csv("/data/consommation_clean")

# Sauvegarder en Parquet partitionné par date.
    # df_clean_with_time.write \
    #     .mode("overwrite") \
    #     .partitionBy("date", "type_energie") \
    #     .parquet("/output/consommations_clean/")
    

    # Rapport final
    print("================ RAPPORT DE NETTOYAGE ===============")
    print(f"Lignes en entree:              {initial_count:>12,}")
    print(f"Timestamps invalides:          {invalid_timestamps:>12,}")
    print(f"Valeurs non numeriques:        {invalid_values:>12,}")
    print(f"Valeurs negatives:             {negative_count:>12,}")
    print(f"Outliers (>10000):              {outlier_count:>12,}")
    print(f"Doublons:                      {duplicates_removed:>12,}")
    total_removed = invalid_timestamps + invalid_values + negative_count + outlier_count + duplicates_removed
    print(f"Total lignes supprimees:       {total_removed:>12,}")

    # Fermer Spark
    spark.stop()


if __name__ == "__main__":
    main()