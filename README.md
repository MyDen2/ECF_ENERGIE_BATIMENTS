# ECF DataPulse Analytics - Pipeline de Données Multi-Sources

## Titre Professionnel Data Engineer - RNCP35288
### Compétences évaluées : C1.1, C1.3, C1.4

---

## Description du projet

Ce projet met en œuvre un pipeline de données complet pour l’analyse des consommations énergétiques de bâtiments, de l’ingestion brute jusqu’à la visualisation et aux recommandations décisionnelles.

L’objectif est de démontrer la capacité à :

Concevoir une architecture data robuste

Traiter des volumes significatifs avec Apache Spark

Enchaîner des traitements analytiques reproductibles

Restituer les résultats sous forme claire et exploitable

## Architecture

```
┌──────────────────┐
│ Données sources  │
│                  │
│ • Consommations  │
│ • Bâtiments      │
│ • Données météo  │
└─────────┬────────┘
          │
          ▼
┌──────────────────────────┐
│ Traitement Spark         │
│                          │
│ • Nettoyage              │
│ • Normalisation          │
│ • Agrégations            │
│ • Parquet partitionné    │
└─────────┬────────────────┘
          │
          ▼
┌──────────────────────────┐
│ Analyse & Visualisation  │
│ (Pandas / Matplotlib /  │
│  Seaborn / Dashboard)    │
└─────────┬────────────────┘
          │
          ▼
┌──────────────────────────┐
│ Restitution              │
│                          │
│ • Graphiques             │
│ • Détection d’anomalies  │
│ • Recommandations        │
│ • Rapport & slides       │
└──────────────────────────┘

```

## Structure du projet

```
ecf_energie/
├── README.md                                # Instructions d'execution
├── data/
│   ├── batiments.csv
│   ├── consommations_raw.csv
│   ├── meteo_raw.csv
│   └── tarifs_energie.csv
├── notebooks/
│   ├── 01_exploration_spark.ipynb
│   ├── 02_nettoyage_spark.py
│   ├── 03_agregations_spark.ipynb
│   ├── 04_nettoyage_meteo_pandas.ipynb
│   ├── 05_fusion_enrichissement.ipynb
│   ├── 06_statistiques_descriptives.ipynb
│   ├── 07_analyse_correlations.ipynb
│   ├── 08_detection_anomalies.ipynb
│   ├── 09_visualisations_matplotlib.ipynb
│   ├── 10_visualisations_seaborn.ipynb
│   └── 11_dashboard_executif.ipynb
├── output/
│   ├── consommations_clean/               # Parquet partitionne
│   ├── consommations_agregees.parquet
│   ├── meteo_clean.csv
│   ├── consommations_enrichies.csv
│   ├── consommations_enrichies.parquet
│   ├── matrice_correlation.csv
│   ├── anomalies_detectees.csv
│   ├── figures/                           # Tous les graphiques
│   └── rapport_synthese.md
├── Dockerfile.pipeline
├── docker-compose.yml
└── requirements.txt
```

## Technologies utilisées

| Domaine              | Technologie             |
| -------------------- | ----------------------- |
| Traitement distribué | Apache Spark 3.5.3      |
| Analyse              | Pandas                  |
| Visualisation        | Matplotlib, Seaborn     |
| Orchestration        | Docker & Docker Compose |
| Formats              | CSV, Parquet            |
| Notebooks            | Jupyter / nbconvert     |


## Installation

### 1. Démarrer l'infrastructure

```bash
# Démarrer tous les services
docker-compose up -d

# Vérifier l'état des services
docker-compose ps
```

### 2. Accès aux interfaces

| Service | URL | Identifiants |
|---------|-----|--------------|
| **Spark Master UI** | http://localhost:8080 |Supervision du cluster Spark (jobs workers, mémoire, CPU) |
| **Spark Application UI** | http://localhost:4040 | Détails d’exécution d’un job Spark en cours |
| **JupyterLab** | http://localhost:8888 | Exploration interactive, analyses et visualisations |

⚠️ Le port 4040 est actif uniquement lorsqu’un job Spark est en cours d’exécution.

Accès JupyterLab

Accès via navigateur :
👉 http://localhost:8888

Les notebooks sont montés automatiquement depuis le dossier :
```bash
./notebooks
```

Les résultats et fichiers générés sont disponibles dans :
```bash
./output
```

### 3. Exécuter le pipeline

```bash
# Pipeline complet
docker compose run --rm pipeline
```
Cette commande exécute automatiquement :

- Nettoyage Spark (02_nettoyage_spark.py)

- Agrégations Spark

- Analyses Pandas

- Détection d’anomalies

- Visualisations

- Dashboard exécutif

- Génération des livrables

## Résulats produits

- Parquet nettoyé et partitionné

- Statistiques descriptives

- Analyse des corrélations

- Détection d’anomalies énergétiques

- Graphiques haute résolution (PNG 300 dpi)

- Dashboard exécutif

- Rapport de synthèse

## Sources de données

Le projet s’appuie sur des fichiers CSV structurés, représentant les consommations énergétiques de bâtiments et leurs caractéristiques.

1️⃣ Consommations énergétiques

Fichier : data_ecf/consommations_raw.csv

Contient les relevés de consommation énergétique par bâtiment.

Colonne	Description
batiment_id	Identifiant unique du bâtiment
timestamp	Date et heure du relevé
type_energie	Type d’énergie (électricité, gaz, eau, etc.)
conso	Consommation brute mesurée
cout	Coût associé à la consommation

Ces données sont brutes, non nettoyées, et peuvent contenir :

- Valeurs aberrantes

- Données manquantes

- Incohérences temporelles

2️⃣ Référentiel bâtiments

Fichier : data_ecf/batiments.csv

Décrit les caractéristiques structurelles des bâtiments.

Colonne	Description
batiment_id	Identifiant du bâtiment
nom	Nom du bâtiment
type	Type de bâtiment (école, mairie, logement, etc.)
commune	Commune d’implantation
surface_m2	Surface en m²
annee_construction	Année de construction
classe_energetique	Classe DPE (A à G)
nb_occupants_moyen	Occupation moyenne

Ce fichier sert de référentiel de jointure pour enrichir les données de consommation.

3️⃣ Données météo (optionnelles)

Les données météorologiques sont utilisées pour :

Analyser l’impact de la température sur la consommation

Identifier des anomalies non liées au comportement des bâtiments

Elles sont traitées via Pandas dans les notebooks analytiques.

## Commandes utiles

```bash
# Logs du pipeline
docker-compose logs -f pipeline

# Arrêter l'infrastructure
docker-compose down

# Supprimer les données (volumes)
docker-compose down -v
```