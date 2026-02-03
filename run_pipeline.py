#!/usr/bin/env python3
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).parent
OUTPUT = ROOT / "output"
LOGS = ROOT / "logs"

def run(cmd: str):
    print(f"\n▶ {cmd}")
    result = subprocess.run(cmd, shell=True)
    if result.returncode != 0:
        print("❌ Erreur lors de l'exécution")
        sys.exit(result.returncode)

def main():
    # Créer les dossiers si besoin
    OUTPUT.mkdir(exist_ok=True)
    LOGS.mkdir(exist_ok=True)

    print("🚀 Démarrage du pipeline ECF énergie")

    # 1) Démarrer le cluster Spark
    run("docker compose up -d spark-master spark-worker-1 spark-worker-2 spark-worker-3")

    # 2) Lancer le job Spark (Partie 1.2)
    run("docker compose run --rm spark-job")

    # 🔜 Plus tard, tu ajouteras ici :
    # run("docker compose run --rm pandas-meteo")
    # run("docker compose run --rm pandas-fusion")

    print("\n✅ Pipeline terminé avec succès")

if __name__ == "__main__":
    main()
