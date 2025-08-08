# ETL - Projet Prefect

Ce projet contient un pipeline ETL qui synchronise les données entre une base SQL Server (On promise) et PostgreSQL (héberjement AWS) en utilisant Prefect pour l'orchestration.

## 📋 Prérequis

- Python 3.8+
- SQL Server
- AWS acces Key / Postgres Acces
- Prefect 3.x

## 🚀 Installation

### 1. Cloner le projet

```bash
git clone <your-repo-url>
cd etl_sqlserver_aws
```

### 2. Créer un environnement virtuel

```bash
python -m venv .venv
# Windows
.venv\Scripts\activate
# Linux/Mac
source .venv/bin/activate
```

### 3. Installer les dépendances

```bash
pip install -r ../requirements.txt
```

## ⚙️ Configuration

### 1. Configuration des bases de données

Modifiez les paramètres de connexion dans :

- `etl_sqlserver_aws/database/sqlserver/get_triggers.py` - Configuration SQL Server
- `etl_sqlserver_aws/database/postgres/pg_connect.py` - Configuration PostgreSQL

### 2. Configuration Prefect

Le projet utilise les fichiers de configuration suivants :

- `etl_sqlserver_aws/prefect.yaml` - Configuration principale du projet
- `etl_sqlserver_aws/deployment.yaml` - Configuration du déploiement

## 🏃‍♂️ Exécution

### Executé un prefect serveur tomporaire (pour un prefect test)

```bash
python main.py
```

### Déploiement avec Prefect

#### a) Initialier un fichie prefect pour la configuration de votre deployment

```bash
prefect init # Va créer un fichie de configuration "prefect.yaml"
```

**Ensuite, modifiez le fichier `prefect.yaml` avec les paramètres suivants :**

```yaml
# Welcome to your prefect.yaml file! You can use this file for storing and managing
# configuration for deploying your flows. We recommend committing this file to source
# control along with your flow code.

# Generic metadata about this project
name: etl_sqlserver_aws
prefect-version: 3.4.10

# build section allows you to manage and build docker images
build: null

# push section allows you to manage if and how this project is uploaded to remote locations
push: null

# pull section allows you to provide instructions for cloning this project in remote locations
pull:
  - prefect.deployments.steps.set_working_directory:
      directory: PATH\TO\..\etl_sqlserver_aws

# the deployments section allows you to provide configuration for deploying flows
deployments: # can runs manually, via schedule, API, Throught an automation
  - name: sorecom-deployment
    version: null
    tags: []
    description: "Déploiement régulier du pipeline SORECOM"
    schedule:
      cron: "*/10 * * * *"
    flow_name: sorecom_pipeline
    entrypoint: main.py:sorecom_pipeline
    parameters: {}
    work_pool:
      name: default-agent-pool
      work_queue_name: default
      job_variables: {}

  - name: cleanup-triggers-monthly
    version: null
    tags: ["maintenance", "monthly"]
    description: "Nettoyage mensuel des tables de triggers (garde 20 dernieres lignes)"
    schedule:
      cron: "0 0 1 * *" # Chaque 1er du mois à 00:00
      timezone: "Etc/UTC"
    flow_name: cleanup_triggers_flow
    entrypoint: main.py:cleanup_triggers_flow
    parameters: {}
    work_pool:
      name: default-agent-pool
      work_queue_name: default
      job_variables: {}
```

### b) Démarrer le serveur Prefect

```bash
prefect server start
```

#### c) Déployer le flow

```bash
prefect deploy --name sorecom-deployment
```

#### d) Démarrer un worker

```bash
export PREFECT_API_URL="http://127.0.0.1:4200/api"
```

```bash
prefect worker start --pool default-agent-pool
```

## 📊 Monitoring

- Interface Prefect : `http://localhost:4200`
- Le pipeline s'exécute toutes les 5 minutes selon la configuration cron `*/5 * * * *`

## 🗂️ Structure du projet

```
etl_sqlserver_aws/
|-- __ini__.py
├── main.py              # Flow principal
├── deploy.py            # Script de déploiement
├── prefect.yaml         # Configuration Prefect
├── deployment.yaml      # Configuration déploiement
└── database/
    ├── sqlserver/       # Connexion et requêtes SQL Server
    |   |-- __init__.py
    │   └── get_triggers.py
    └── postgres/        # Connexion et opérations PostgreSQL
        |-- __init__.py
        ├── pg_connect.py
        ├── upload_article.py
        ├── upload_famille.py
        └── upload_stk_reel.py
```

## 🔄 Fonctionnement

Le pipeline `sorecom_pipeline` :

1. **Détecte les changements** récents (5 dernières minutes) via les tables trigger
2. **Traite les familles** : Création/modification/suppression des catégories
3. **Traite les articles** : Synchronisation des produits
4. **Met à jour les stocks** : Actualisation des quantités disponibles

## 🛠️ Commandes utiles

```bash
# Voir les déploiements
prefect deployment ls

# Voir les flows
prefect flow ls

# Voir les runs
prefect flow-run ls

# Supprimer un déploiement
prefect deployment delete <deployment-name>

# Logs en temps réel
prefect flow-run logs --tail <run-id>
```

## 🐛 Dépannage

1. **Erreur de connexion SQL Server** : Vérifiez les paramètres dans `get_triggers.py`
2. **Erreur de connexion PostgreSQL** : Vérifiez les paramètres dans `pg_connect.py`
3. **Worker non démarré** : Assurez-vous qu'un worker est actif avec `prefect worker start`

## 📝 Notes

- Les fichiers sensibles (`encrypt.py`, `deploy.py`) sont exclus du Git via `.gitignore`
- Le pipeline filtre automatiquement les données des 5 dernières minutes
- Les logs sont disponibles dans l'interface Prefect pour le monitoring

## 🔒 Sécurité

- Les informations de connexion aux bases de données doivent être configurées de manière sécurisée
- Utilisez des variables d'environnement pour les mots de passe et clés sensibles
- Les fichiers de configuration contenant des credentials sont exclus du contrôle de version
