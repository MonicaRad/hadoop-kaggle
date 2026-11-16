# TP Big Data Lakehouse — HDFS (3 DataNodes) + PySpark + Delta Lake + Streamlit

Environnement pédagogique complet compatible nativement **`linux/amd64` (PC x86_64 / WSL2)** et **`linux/arm64` (Apple Silicon M1–M4 / Linux ARM64)** sur **Ubuntu 26.04 LTS**, reposant sur un **cluster HDFS 3.5 réel distribué (1 NameNode + 3 DataNodes)** avec blocs réduits à **16 Mo** et détection de panne abaissée à **~50 secondes**, couplé à une image mutualisée légère (`ubuntu:26.04` + **Python 3.15** via `uv` + OpenJDK 21 + JARs Delta pré-embarqués) pour **PySpark 4.2**, **Delta Lake 4.4**, **JupyterLab 4.6** et **Streamlit**.

Ce corrigé de référence croise **deux jeux de données publics Kaggle** via jointure distribuée sur `seller_id` :
1. **Olist Brazilian E-Commerce** (`olistbr/brazilian-ecommerce`, ~100 000 commandes B2C sur 9 tables relationnelles).
2. **Olist Marketing Funnel** (`olistbr/marketing-funnel-olist`, entonnoir d'acquisition B2B des marchands sur 2 tables).

```mermaid
flowchart LR
    K1["Dataset #1 : Olist E-Commerce<br/>data_raw/brazilian-ecommerce/"] --> B["Bronze (11 tables Delta)<br/>/lakehouse/bronze"]
    K2["Dataset #2 : Marketing Funnel<br/>data_raw/marketing-funnel-olist/"] --> B
    B -->|"Jointure Spark (seller_id)"| S["Silver (ACID Delta)<br/>/lakehouse/silver"]
    S --> G["Gold (KPI, Canaux & RFM)<br/>/lakehouse/gold"]
    G --> ST["Dashboard Streamlit<br/>(:8501)"]

    subgraph HDFS["Cluster HDFS (blocs 16 Mo, réplication = 2)"]
      NN["NameNode (:9870 / :9000)"]
      DN1["DataNode 1"]
      DN2["DataNode 2"]
      DN3["DataNode 3"]
    end

    B -.-> HDFS
    S -.-> HDFS
    G -.-> HDFS
```

---

## Arborescence du dépôt

```text
.
├── .devcontainer/
│   └── devcontainer.json       # Connexion VS Code directe au conteneur spark-jupyter
├── .vscode/
│   ├── extensions.json         # Extensions recommandées (Python, Jupyter, Ruff, Docker, Task)
│   ├── launch.json             # Débogage PySpark & prévisualisation Streamlit (:8502)
│   ├── settings.json           # Kernel Python/Jupyter pré-configuré, Ruff & propreté éditeur
│   └── tasks.json              # Actions VS Code en 1 clic (Démarrer, Crash-test, Tests...)
├── docker-compose.yml          # Cluster HDFS (1 NN + 3 DN) + JupyterLab + Streamlit
├── Taskfile.yml                # Orchestration go-task (démarrage, crash-test, tests J1..J4)
├── mise.toml                   # Gestionnaire d'outils (go-task, uv, python 3.15)
├── pyproject.toml              # Dépendances Python compatibles uv (Python >= 3.15)
├── config/
│   ├── core-site.xml           # Point d'entrée RPC hdfs://namenode:9000
│   ├── hdfs-site.xml           # Blocs 16 Mo, réplication=2, détection panne ~50s
│   └── spark-defaults.conf     # Pré-configuration automatique HDFS + Delta Lake
├── docker/
│   ├── hadoop/
│   │   └── Dockerfile          # Image HDFS 3.5.0 Ubuntu 26.04 native multi-arch (amd64 & arm64)
│   └── workspace/
│       ├── Dockerfile          # Image Ubuntu 26.04 multi-arch (Python 3.15 + uv + OpenJDK 21 + Delta 4.4)
│       └── requirements.txt    # Dépendances Python 3.15 pour le workspace
├── data_raw/                   # Sous-dossiers multi-datasets Kaggle avant ingestion HDFS
├── notebooks/                  # Notebooks TP corrigés pas-à-pas (Jours 1 à 4)
│   ├── J1_HDFS_Ingestion_Bronze.ipynb
│   ├── J2_Spark_Delta_Silver.ipynb
│   ├── J3_Analytics_SQL_Gold.ipynb
│   └── J4_Stats_Segmentation_Dashboard.ipynb
└── app/
    └── dashboard.py            # Dashboard Streamlit connecté à /lakehouse/gold/
```

---

## Démarrage rapide

### Avec `mise` et `go-task` (recommandé)

```bash
# 1. Installer automatiquement task (go-task) et uv si absents
mise trust && mise install

# 2. Construire l'image et démarrer tout le cluster (attend la disponibilité des 3 DataNodes)
task up

# 3. Afficher toutes les commandes disponibles
task
```

### Sans `task` (commandes Docker natives)

```bash
docker compose up -d --build
```

---

## Interfaces exposées

| Service | URL | Détails |
|---|---|---|
| **HDFS NameNode UI** | http://localhost:9870 | Onglets *Datanodes* & *Utilities > Browse the file system* |
| **JupyterLab** | http://localhost:8888 | Mot de passe / Token : `bigdata` |
| **Spark Web UI** | http://localhost:4040 | DAG, Stages & Shuffle *(actif dès qu'une `SparkSession` tourne)* |
| **Dashboard Streamlit** | http://localhost:8501 | Lecture des tables Delta de `/lakehouse/gold/` avec cache 60s |

---

## Travailler dans VS Code (Kernel Jupyter & Conteneur pré-configurés)

Deux modes de travail sont prêts à l'emploi selon la RAM disponible sur la machine étudiante :

### Option A — Mode Dev Container intégré *(recommandé : auto-complétion PySpark, Ruff, Git & Kernel en 1 clic)*
1. Ouvrez ce dossier dans VS Code et installez l'extension recommandée **Dev Containers** (`ms-vscode-remote.remote-containers`).
2. Cliquez sur **Reopen in Container** (ou `Ctrl+Shift+P` → `Dev Containers: Reopen in Container`).
3. VS Code démarre automatiquement tout le `docker-compose.yml` et vous connecte directement dans `spark-jupyter` (`/workspace`) :
   - Le kernel **`Python 3.15 (PySpark 4.2 + Delta Lake 4.4)`** (`/usr/local/bin/python3`) est sélectionné et approuvé d'office.
   - L'auto-complétion Pylance (`pyspark`, `delta`, `streamlit`, `scipy`, `sklearn`), le formateur **Ruff**, `git`, `jq` et `pytest` sont pré-installés.
   - Les modifications enregistrées dans `app/dashboard.py` rechargent automatiquement le dashboard Streamlit sur `http://localhost:8501`.

### Option B — Mode Léger depuis l'hôte *(idéal pour les PC portables à 8 Go de RAM)*
1. Lancez `task up` (ou via `Ctrl+Shift+P` → `Tasks: Run Build Task`).
2. Ouvrez un notebook dans `notebooks/`, cliquez sur **Select Kernel** en haut à droite → **Existing Jupyter Server...**
3. Saisissez `http://localhost:8888/?token=bigdata` puis choisissez **`Python 3.15 (PySpark 4.2 + Delta Lake 4.4)`**.

---

## Programme des Notebooks (`notebooks/`)

1. **`J1_HDFS_Ingestion_Bronze.ipynb` — Acquisition multi-datasets, HDFS & Tolérance aux pannes**
   - Téléchargement automatique des 2 datasets Kaggle (`olistbr/brazilian-ecommerce` + `olistbr/marketing-funnel-olist`) dans `data_raw/<source>/`.
   - Copie récursive sur HDFS, inspection du découpage en **blocs de 16 Mo** et de la **Data Locality** (`1 bloc = 1 partition Spark`).
   - **Crash-test en direct** : arrêt de `datanode2`, détection en ~50s, auto-réplication sur le 3e DataNode et lecture sans interruption.
   - Création des 11 tables Delta de la couche **Bronze** (`/lakehouse/bronze/`) avec lignage `_source_dataset` et `_source_file`.
2. **`J2_Spark_Delta_Silver.ipynb` — Jointures Multi-Datasets, Shuffle & Transactions ACID**
   - Nettoyage, typage fort, harmonisation de la clé inter-datasets `seller_id` et prévention du produit cartésien N-M.
   - Jointure distribuée **Broadcast Hash Join** (`F.broadcast`) entre les deux datasets Kaggle et création de `/lakehouse/silver/orders` + `/lakehouse/silver/order_items`.
   - Exploration du `_delta_log`, **Schema Enforcement**, **Upsert (`MERGE INTO`)** et **Time Travel (`VERSION AS OF`)**.
3. **`J3_Analytics_SQL_Gold.ipynb` — Spark SQL avancé, Rigueur statistique & Couche Gold**
   - Asymétrie des délais logistiques : pourquoi privilégier **Médiane (`PERCENTILE_APPROX`) + IQR** face à la moyenne.
   - Fonctions de fenêtrage SQL (`DENSE_RANK`, courbe cumulée de **Pareto**, `ROW_NUMBER`, croissance mensuelle `LAG`).
   - Alimentation de `/lakehouse/gold/{kpi_categories, kpi_states, kpi_monthly, kpi_acquisition_channels}` et compaction `OPTIMIZE`.
4. **`J4_Stats_Segmentation_Dashboard.ipynb` — Tests statistiques, Segmentation RFM & Dashboard**
   - Tests non-paramétriques **Mann-Whitney U**, corrélation de **Spearman** et test multi-datasets **Kruskal-Wallis** (panier selon le canal d'acquisition B2B).
   - Pipeline **RFM + K-Means** (`log1p` + `StandardScaler`) par `customer_unique_id` vers `/lakehouse/gold/customer_rfm`.
   - Mise à jour en direct du tableau de bord Streamlit sur `http://localhost:8501`.

---

## Commandes `task` principales (`Taskfile.yml`)

```bash
task up                                                                                # Démarrer le cluster et attendre les 3 DataNodes + Jupyter + Streamlit
task status                                                                            # État des conteneurs et rapport HDFS (dfsadmin -report)
task data:download DATASET="olistbr/brazilian-ecommerce,olistbr/marketing-funnel-olist" # Télécharger les 2 datasets Olist dans data_raw/<nom>/
task hdfs:ls                                                                           # Lister l'occupation disque des couches Bronze / Silver / Gold
task hdfs:fsck                                                                         # Inspecter les blocs de 16 Mo et leurs DataNodes hôtes
task hdfs:crash-dn2                                                                    # [TP Jour 1] Couper datanode2 pour observer l'auto-réparation en ~50s
task hdfs:restore-dn2                                                                  # [TP Jour 1] Relancer datanode2
task test:docker                                                                       # Vérifier automatiquement la santé de HDFS, JupyterLab et Streamlit
task test:notebooks                                                                    # Exécuter et valider de bout en bout les 4 notebooks (J1 -> J4)
task test:all                                                                          # Validation complète (infrastructure + notebooks + tables Gold)
task down                                                                              # Arrêter les conteneurs (conserve les volumes HDFS)
task clean                                                                             # Arrêter et purger les volumes HDFS
```
