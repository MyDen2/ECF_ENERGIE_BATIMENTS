# from pyspark.sql import SparkSession
# from pyspark.sql import functions as F
# from pyspark.sql.types import DoubleType, TimestampType
# from datetime import datetime
# import os
# import sys

# # Configuration des chemins
# SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
# DATA_DIR = os.path.join(SCRIPT_DIR, "..", "data_ecf")
# OUTPUT_DIR = os.path.join(SCRIPT_DIR, "..", "output", "consommations_clean")

# CONSUMPTION_PATH = os.path.join(DATA_DIR, "consommations_raw.csv")
# BUILDINGS_PATH = os.path.join(DATA_DIR, "batiments.csv")


# # Creer des UDF pour parser les timestamps multi-formats

# def create_spark_session():
#     """Cree et configure la session Spark."""
#     spark = SparkSession.builder \
#         .appName("ECF2 - Nettoyage") \
#         .master("local[*]") \
#         .config("spark.driver.memory", "4g") \
#         .config("spark.sql.shuffle.partitions", "8") \
#         .getOrCreate()

#     spark.sparkContext.setLogLevel("WARN")
#     return spark


# def parse_multi_format_timestamp(timestamp_str):
#     """
#     UDF pour parser les timestamps multi-formats.
#     Formats supportes:
#     - %Y-%m-%d %H:%M:%S (ISO)
#     - %d/%m/%Y %H:%M (FR)
#     - %m/%d/%Y %H:%M:%S (US)
#     - %Y-%m-%dT%H:%M:%S (ISO avec T)
#     """
#     if timestamp_str is None:
#         return None

#     formats = [
#         "%Y-%m-%d %H:%M:%S",
#         "%d/%m/%Y %H:%M",
#         "%m/%d/%Y %H:%M:%S",
#         "%Y-%m-%dT%H:%M:%S",
#     ]

#     for fmt in formats:
#         try:
#             return datetime.strptime(timestamp_str, fmt)
#         except ValueError:
#             continue

#     return None

# # Convertir les valeurs avec virgule en float

# def clean_value(value_str):
#     """
#     UDF pour nettoyer les valeurs numeriques.
#     - Remplace la virgule par un point
#     - Retourne None pour les valeurs non numeriques
#     """
#     if value_str is None:
#         return None

#     try:
#         # Remplacer virgule par point
#         clean_str = value_str.replace(",", ".")
#         return float(clean_str)
#     except (ValueError, AttributeError):
#         return None
    

# def main():
#     # Creer la session Spark
#     spark = create_spark_session()
#     print(f"Spark version: {spark.version}")

#     # Enregistrer les UDFs
#     parse_timestamp_udf = F.udf(parse_multi_format_timestamp, TimestampType())
#     clean_value_udf = F.udf(clean_value, DoubleType())

#     # Charger les donnees brutes
#     print("\n[1/6] Chargement des donnees brutes...")
#     df_raw = spark.read \
#         .option("header", "true") \
#         .csv(CONSUMPTION_PATH)

#     initial_count = df_raw.count()
#     print(f"  Lignes en entree: {initial_count:,}")

#     # Parser les timestamps multi-formats
#     print("\n[2/6] Parsing des timestamps multi-formats...")
#     df_with_timestamp = df_raw.withColumn(
#         "timestamp_parsed",
#         parse_timestamp_udf(F.col("timestamp"))
#     )

#     df_with_timestamp = (
#         df_with_timestamp
#         .drop("timestamp")                      # supprime le timestamp string
#         .withColumnRenamed("timestamp_parsed", "timestamp")  # garde le timestamp propre
#     )

#     # Filtrer les timestamps invalides
#     invalid_timestamps = df_with_timestamp.filter(F.col("timestamp").isNull()).count()
#     df_with_timestamp = df_with_timestamp.filter(F.col("timestamp").isNotNull())
#     print(f"  Timestamps invalides supprimes: {invalid_timestamps:,}")

#     # Convertir les valeurs avec virgule decimale en float
#     print("\n[3/6] Conversion des valeurs numeriques...")
#     df_with_values = df_with_timestamp.withColumn(
#         "conso_clean",
#         clean_value_udf(F.col("consommation"))
#     )

#     # Filtrer les valeurs non numeriques
#     invalid_values = df_with_values.filter(F.col("conso_clean").isNull()).count()
#     df_with_values = df_with_values.filter(F.col("conso_clean").isNotNull())
#     print(f"  Valeurs non numeriques supprimees: {invalid_values:,}")

#     # Supprimer les valeurs negatives et les outliers (>10000)
#     print("\n[4/6] Suppression des valeurs aberrantes...")
#     negative_count = df_with_values.filter(F.col("conso_clean") < 0).count()
#     outlier_count = df_with_values.filter(F.col("conso_clean") > 10000).count()

#     df_clean = df_with_values.filter(
#         (F.col("conso_clean") >= 0) & (F.col("conso_clean") <= 10000)
#     )
#     print(f"  Valeurs negatives supprimees: {negative_count:,}")
#     print(f"  Outliers (>10000) supprimes: {outlier_count:,}")

#     # Dedupliquer sur (batiment_id, timestamp, type_energie)
#     print("\n[5/6] Deduplication...")
#     before_dedup = df_clean.count()
#     df_dedup = df_clean.dropDuplicates(["batiment_id", "timestamp", "type_energie"])
#     after_dedup = df_dedup.count()
#     duplicates_removed = before_dedup - after_dedup
#     print(f"  Doublons supprimes: {duplicates_removed:,}")

#     # Calculer les moyennes horaires par station et polluant
#     print("\n[6/6] Agregation horaire et sauvegarde...")

#     # Ajouter les colonnes de temps
#     df_with_time = df_dedup.withColumn(
#         "date", F.to_date(F.col("timestamp"))
#     ).withColumn(
#         "hour", F.hour(F.col("timestamp"))
#     ).withColumn(
#         "year", F.year(F.col("timestamp"))
#     ).withColumn(
#         "month", F.month(F.col("timestamp"))
#     ).withColumn(
#         "day", F.day(F.col("timestamp"))
#     )

#     # print("df with time hour day month year")
#     # df_with_time.show(20)

#     # Agreger par heure
#     df_hourly_consumption_by_building = df_with_time.groupBy(
#         #"batiment_id", "type_energie", "hour", 'unite'
#         "batiment_id", "hour", "type_energie"
#     ).agg(
#         F.round(F.mean("conso_clean"), 2).alias("conso_hour_avg")
#     )
    
#     # print("df par heure")
#     # df_hourly_consumption_by_building.show(20)


#     #Consommations journalieres par batiment et type d'energie
#     df_daily_consumption_by_building = df_with_time.groupBy(
#         "batiment_id", "day", "type_energie"
#     ).agg(
#         F.round(F.mean("conso_clean"), 2).alias("conso_daily_avg")
#     )
    
#     # print("df par jour")
#     # df_daily_consumption_by_building.show(20)

#     # # Charger les batiments pour calculer les aggrégations
#     df_buildings = spark.read \
#         .option("header", "true") \
#         .option("inferSchema", "true") \
#         .csv(BUILDINGS_PATH)

#     # Consommations mensuelles par commune
#     df_monthly_consumption_by_city = (
#     df_with_time
#     .join(
#         df_buildings.select("batiment_id", "commune"),
#         on="batiment_id",
#         how="left"
#     )
#     .groupBy("commune", "type_energie", "month")
#     .agg(
#         F.round(F.sum("conso_clean"), 2).alias("conso_month_sum")
#     )
#     )

#     print("\n[7/7] Ecriture Parquet partitionné (date, type_energie)...")

#     # Dataset final "clean" à écrire
#     df_to_write = (
#         df_with_time
#         .select(
#             "batiment_id",
#             "timestamp",
#             "date",
#             "type_energie",
#             "conso_clean",
#             "unite"
#         )
#     )

#     # Ecriture Parquet partitionnée
#     (df_to_write.write
#         .mode("overwrite")
#         .partitionBy("date", "type_energie")
#         .parquet(OUTPUT_DIR)
#     )

#     rows_out = df_to_write.count()
#     print(f"  Parquet écrit dans: {OUTPUT_DIR}")
#     print(f"  Lignes en sortie: {rows_out:,}")

#     # Rapport final
#     print("RAPPORT DE NETTOYAGE")
#     print(f"Lignes en entree:              {initial_count:>12,}")
#     print(f"Timestamps invalides:          {invalid_timestamps:>12,}")
#     print(f"Valeurs non numeriques:        {invalid_values:>12,}")
#     print(f"Valeurs negatives:             {negative_count:>12,}")
#     print(f"Outliers (>1000):              {outlier_count:>12,}")
#     print(f"Doublons:                      {duplicates_removed:>12,}")
#     total_removed = invalid_timestamps + invalid_values + negative_count + outlier_count + duplicates_removed
#     print(f"Total lignes supprimees:       {total_removed:>12,}")
#     print(f"\nFichiers Parquet sauvegardes dans: {OUTPUT_DIR}")

#     # Fermer Spark
#     spark.stop()


# if __name__ == "__main__":
#     main()

#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType


# -----------------------
# Logging
# -----------------------
def log_line(log_path: str, msg: str) -> None:
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(log_path, "a", encoding="utf-8") as f:
        f.write(f"[{ts}] {msg}\n")


# -----------------------
# Args
# -----------------------
def parse_args():
    p = argparse.ArgumentParser(description="ECF Energie - 02 Nettoyage Spark (C2.2)")
    p.add_argument("--input", required=True, help="CSV consommations_raw.csv")
    p.add_argument("--buildings", required=True, help="CSV batiments.csv (pour agrégation mensuelle par commune)")
    p.add_argument("--output", required=True, help="Dossier de sortie parquet clean")
    p.add_argument("--log", required=True, help="Fichier log")
    p.add_argument("--outlier-threshold", type=float, default=10000.0, help="Seuil outliers (default 10000)")
    p.add_argument("--master", default="local[*]", help="Spark master (local[*] ou spark://spark-master:7077)")
    p.add_argument("--driver-memory", default="4g", help="Spark driver memory (default 4g)")
    p.add_argument("--shuffle-partitions", type=int, default=8, help="spark.sql.shuffle.partitions (default 8)")
    p.add_argument("--mode", default="overwrite", choices=["overwrite", "append"], help="Mode d'écriture parquet")
    p.add_argument(
        "--write-aggs",
        action="store_true",
        help="Si présent, écrit aussi les agrégations dans <output>_agg/"
    )
    return p.parse_args()


# -----------------------
# Spark
# -----------------------
def create_spark_session(master: str, driver_memory: str, shuffle_partitions: int) -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("ECF - Nettoyage Consommations")
        .master(master)
        .config("spark.driver.memory", driver_memory)
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


# -----------------------
# UDF timestamp (multi-formats)
# -----------------------
def build_timestamp_udf():
    """
    UDF pour parser les timestamps multi-formats (exigence ECF).
    """
    import datetime as dt

    formats = [
        "%Y-%m-%d %H:%M:%S",   # ISO
        "%Y-%m-%dT%H:%M:%S",   # ISO avec T
        "%d/%m/%Y %H:%M",      # FR
        "%m/%d/%Y %H:%M",      # US sans secondes
        "%m/%d/%Y %H:%M:%S",   # US avec secondes
        "%Y-%m-%d %H:%M",      # ISO sans secondes
    ]

    def parse_ts(s):
        if s is None:
            return None
        s = str(s).strip()
        if s == "":
            return None
        for fmt in formats:
            try:
                return dt.datetime.strptime(s, fmt)
            except Exception:
                pass
        return None

    return F.udf(parse_ts, TimestampType())


# -----------------------
# Main pipeline
# -----------------------
def main():
    args = parse_args()

    # reset log file
    open(args.log, "w", encoding="utf-8").close()
    log_line(args.log, f"START input={args.input} buildings={args.buildings} output={args.output}")
    log_line(args.log, f"PARAMS master={args.master} driver_memory={args.driver_memory} shuffle={args.shuffle_partitions} "
                       f"outlier_threshold={args.outlier_threshold} mode={args.mode}")

    spark = create_spark_session(args.master, args.driver_memory, args.shuffle_partitions)
    log_line(args.log, f"Spark version={spark.version}")

    # 1) Read raw
    df_raw = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false") 
        .csv(args.input)
    )

    rows_in = df_raw.count()
    log_line(args.log, f"READ rows_in={rows_in}")

    # 2) Parse timestamps via UDF + garder un seul timestamp propre
    ts_udf = build_timestamp_udf()

    df_ts = (
        df_raw
        .withColumn("timestamp_parsed", ts_udf(F.col("timestamp")))
        .drop("timestamp")  
        .withColumnRenamed("timestamp_parsed", "timestamp")  # timestamp propre unique
    )

    bad_ts = df_ts.filter(F.col("timestamp").isNull()).count()
    df_ts_ok = df_ts.filter(F.col("timestamp").isNotNull())
    rows_after_ts = df_ts_ok.count()
    log_line(args.log, f"TIMESTAMP bad={bad_ts} remaining={rows_after_ts}")

    # 3) Clean conso numeric (Spark 3.4/3.5 strict => try_cast)
    # Remplace virgule -> point, trim, try_cast double
    df_num = df_ts_ok.withColumn(
        "conso_clean",
        F.expr("try_cast(regexp_replace(trim(cast(consommation as string)), ',', '.') as double)")
    )

    bad_conso = df_num.filter(F.col("conso_clean").isNull()).count()
    df_num_ok = df_num.filter(F.col("conso_clean").isNotNull())
    rows_after_conso = df_num_ok.count()
    log_line(args.log, f"CONSO non_castable={bad_conso} remaining={rows_after_conso}")

    # 4) Filter negatives + outliers
    neg_count = df_num_ok.filter(F.col("conso_clean") < 0).count()
    out_count = df_num_ok.filter(F.col("conso_clean") > F.lit(args.outlier_threshold)).count()

    df_range_ok = df_num_ok.filter(
        (F.col("conso_clean") >= 0) &
        (F.col("conso_clean") <= F.lit(args.outlier_threshold))
    )
    rows_after_range = df_range_ok.count()
    log_line(args.log, f"FILTER negatives={neg_count} outliers={out_count} remaining={rows_after_range}")

    # 5) Deduplicate
    before_dedup = rows_after_range
    df_dedup = df_range_ok.dropDuplicates(["batiment_id", "timestamp", "type_energie"])
    after_dedup = df_dedup.count()
    dedup_removed = before_dedup - after_dedup
    log_line(args.log, f"DEDUP removed={dedup_removed} remaining={after_dedup}")

    # 6) Add time columns (keys correctes pour agrégations et partitions)
    df_time = (
        df_dedup
        .withColumn("date", F.to_date(F.col("timestamp")))                     # partition jour
        .withColumn("hour_ts", F.date_trunc("hour", F.col("timestamp")))       # clé horaire complète
        .withColumn("month_ts", F.date_trunc("month", F.col("timestamp")))     # clé mensuelle complète
    )

    # 7) Aggregations demandées (correctes)
    # 7a) Consommations horaires moyennes par bâtiment (et par énergie)
    df_hourly = (
        df_time
        .groupBy("batiment_id", "type_energie", "hour_ts")
        .agg(F.round(F.mean("conso_clean"), 2).alias("conso_hour_avg"))
    )

    # 7b) Consommations journalières par bâtiment et type d’énergie (somme)
    df_daily = (
        df_time
        .groupBy("batiment_id", "type_energie", "date")
        .agg(F.round(F.sum("conso_clean"), 2).alias("conso_day_sum"))
    )

    # 7c) Consommations mensuelles par commune (nécessite jointure batiments)
    df_buildings = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .csv(args.buildings)
        .select("batiment_id", "commune")
    )

    df_monthly_commune = (
        df_time
        .join(df_buildings, on="batiment_id", how="left")
        .groupBy("commune", "type_energie", "month_ts")
        .agg(F.round(F.sum("conso_clean"), 2).alias("conso_month_sum"))
    )

    # Audit: communes manquantes
    null_commune = df_monthly_commune.filter(F.col("commune").isNull()).count()
    log_line(args.log, f"JOIN buildings null_commune_groups={null_commune}")

    # 8) Write Parquet partitionné (date, type_energie)
    MAX_ROWS = 100000
    df_to_write = df_time.select(
        "batiment_id",
        "timestamp",
        "date",
        "type_energie",
        "conso_clean",
        "unite"
    ).limit(MAX_ROWS)

    (df_to_write.write
        .mode(args.mode)
        .partitionBy("date", "type_energie")
        .parquet(args.output)
    )

    rows_out = df_to_write.count()
    log_line(args.log, f"WRITE parquet rows_out={rows_out} path={args.output}")

    # 9) Optionnel: écrire aussi les agrégats
    # if args.write_aggs:
    #     agg_base = args.output.rstrip("/") + "_agg"
    #     df_hourly.write.mode("overwrite").parquet(f"{agg_base}/hourly_by_building")
    #     df_daily.write.mode("overwrite").parquet(f"{agg_base}/daily_by_building")
    #     df_monthly_commune.write.mode("overwrite").parquet(f"{agg_base}/monthly_by_commune")
    #     log_line(args.log, f"WRITE aggs path={agg_base}")

    # 10) Résumé console + log (entrée vs sortie = métrique réelle)
    removed_real = rows_in - rows_out

    # print("\n==================== RAPPORT NETTOYAGE (C2.2) ====================")
    # print(f"Entrée (rows_in)                        : {rows_in:,}")
    # print(f"Sortie (rows_out)                       : {rows_out:,}")
    # print(f"Supprimées (réel entrée-sortie)          : {removed_real:,}")
    # print("---- Détails (par étape) ----")
    # print(f"Timestamps invalides (bad_ts)            : {bad_ts:,}")
    # print(f"Consommations non castables (bad_conso)  : {bad_conso:,}")
    # print(f"Valeurs négatives                        : {neg_count:,}")
    # print(f"Outliers > {args.outlier_threshold:g}                 : {out_count:,}")
    # print(f"Doublons supprimés                       : {dedup_removed:,}")
    # print(f"\nParquet partitionné écrit dans           : {args.output}")
    # if args.write_aggs:
    #     print(f"Agrégats écrits dans                      : {args.output}_agg/")
    # print("==================================================================\n")

    # log_line(args.log, f"SUMMARY rows_in={rows_in} rows_out={rows_out} removed_real={removed_real}")
    # log_line(args.log, "END SUCCESS")

    log_line(args.log, "==================== RAPPORT NETTOYAGE (C2.2) ====================")
    log_line(args.log, f"Entrée (rows_in)                        : {rows_in}")
    log_line(args.log, f"Sortie (rows_out)                       : {rows_out}")
    log_line(args.log, f"Supprimées (réel entrée-sortie)          : {removed_real}")
    log_line(args.log, "---- Détails (par étape) ----")
    log_line(args.log, f"Timestamps invalides (bad_ts)            : {bad_ts}")
    log_line(args.log, f"Consommations non castables (bad_conso)  : {bad_conso}")
    log_line(args.log, f"Valeurs négatives                        : {neg_count}")
    log_line(args.log, f"Outliers > {args.outlier_threshold}      : {out_count}")
    log_line(args.log, f"Doublons supprimés                       : {dedup_removed}")
    log_line(args.log, f"Parquet écrit dans                       : {args.output}")
    if args.write_aggs:
        log_line(args.log, f"Agrégats écrits dans                     : {args.output}_agg/")
    log_line(args.log, "===================================================================")
    log_line(args.log, "END SUCCESS")
    spark.stop()


if __name__ == "__main__":
    main()

