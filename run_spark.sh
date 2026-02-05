# #!/bin/bash
# set -e

# echo "🚀 PIPELINE ECF ÉNERGIE - DÉMARRAGE (Spark)"

# echo "▶ Étape 1 : Nettoyage Spark"
# spark-submit \
#   --master spark://spark-master:7077 \
#   /notebooks/02_nettoyage_spark.py \
#   --input /data_ecf/consommations_raw.csv \
#   --buildings /data_ecf/batiments.csv \
#   --output /output/consommations_clean \
#   --log /logs/02_nettoyage_spark.log \
#   --outlier-threshold 10000

# echo "✅ Étape Spark terminée"


#!/bin/bash
set -e

echo "🚀 PIPELINE ECF ÉNERGIE - DÉMARRAGE (Spark)"

echo "▶ Étape 1 : Nettoyage Spark"
/opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /notebooks/02_nettoyage_spark.py \
  --input /data_ecf/consommations_raw.csv \
  --buildings /data_ecf/batiments.csv \
  --output /output/consommations_clean \
  --log /logs/02_nettoyage_spark.log \
  --outlier-threshold 10000

echo "✅ Étape Spark terminée"
