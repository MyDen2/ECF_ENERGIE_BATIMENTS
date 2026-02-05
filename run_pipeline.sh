#!/bin/bash
set -e

echo "🚀 PIPELINE ECF ÉNERGIE - DÉMARRAGE (FULL)"

bash /run_spark.sh
bash /run_notebooks.sh

echo "✅ PIPELINE COMPLET TERMINÉ"
