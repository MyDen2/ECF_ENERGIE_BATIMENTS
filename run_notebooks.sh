#!/bin/bash
set -e

echo "🚀 PIPELINE ECF ÉNERGIE - DÉMARRAGE (Notebooks)"

echo "▶ Étape 2 : Exécution des notebooks"

for nb in \
  03_agregations_spark.ipynb \
  04_nettoyage_meteo_pandas.ipynb \
  05_fusion_enrichissement.ipynb \
  06_statistiques_descriptives.ipynb \
  07_analyse_correlations.ipynb \
  08_detection_anomalies.ipynb \
  09_visualisations_matplotlib.ipynb \
  10_visualisations_seaborn.ipynb \
  11_dashboard_executif.ipynb
do
  echo "▶ Exécution $nb"
  python -m jupyter nbconvert \
    --to notebook \
    --execute \
    --inplace \
    /notebooks/$nb
done

echo "✅ Étape Notebooks terminée"

