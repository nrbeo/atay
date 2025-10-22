# DataEng 2024 Template Repository

![Insalogo](./images/logo-insa_0.png)

Project [DATA Engineering](https://www.riccardotommasini.com/courses/dataeng-insa-ot/) is provided by [INSA Lyon](https://www.insa-lyon.fr/).



## Overview

Ce projet met en place une architecture de data engineering complète basée sur **Apache Airflow** pour l’orchestration des pipelines, **PostgreSQL** pour le stockage, **Streamlit** pour la visualisation et **PgAdmin** pour l’administration de la base.

L’ensemble est entièrement conteneurisé avec **Docker Compose** et automatisé via un **Makefile**.

---

##  Architecture

### Schéma général :

```
              ┌──────────────┐
              │  Streamlit   │  ← Interface Web pour visualiser les données
              └──────┬───────┘
                     │
                     ▼
        ┌─────────────────────┐
        │   Apache Airflow    │  ← Orchestration des pipelines ETL
        │ (Scheduler, Worker) │
        └────────┬────────────┘
                 │
                 ▼
        ┌─────────────────────┐
        │    PostgreSQL DB    │  ← Stockage persistant
        └────────┬────────────┘
                 │
                 ▼
        ┌─────────────────────┐
        │       Redis         │  ← Broker Celery pour Airflow
        └─────────────────────┘
```

### Services principaux :

| Service        | Port   | Description                           |
| -------------- | ------ | ------------------------------------- |
| **Airflow UI** | `8080` | Interface d’administration des DAGs   |
| **PgAdmin**    | `5050` | Interface d’administration PostgreSQL |
| **Streamlit**  | `8501` | Interface front-end pour la data      |
| **PostgreSQL** | `5432` | Base de données relationnelle         |
| **Redis**      | `6379` | Broker de tâches Celery               |

---

## Préparation de l’environnement

### Cloner le dépôt

```bash
git clone <url-du-repo>
cd atay
```

### Créer le fichier `.env`

Dans le dossier `docker/config/`, tu trouveras un fichier modèle :

```
docker/config/.env.example
```

Copie-le et renomme-le en :

```bash
cp docker/config/.env.example docker/config/.env
```

Vérifie ou adapte les valeurs si nécessaire (par exemple les ports ou mots de passe).

---

## Lancer le projet avec le Makefile

Toutes les commandes principales sont regroupées dans le fichier `Makefile`.

### Initialiser Airflow

(à exécuter **une seule fois**)

```bash
make init-airflow
```

### Démarrer tous les services

```bash
make run-airflow
```

Tu peux ensuite accéder à :

* **Airflow UI** → [http://localhost:8080](http://localhost:8080)
* **Streamlit App** → [http://localhost:8501](http://localhost:8501)
* **PgAdmin** → [http://localhost:5050](http://localhost:5050)

### Stopper les conteneurs

```bash
make stop
```

### Stopper et supprimer les volumes

```bash
make stop-with-volumes
```

### Nettoyer les dossiers de données

```bash
make clean
```

### Exécuter manuellement un script ETL localement

```bash
make run-etl
```

### Lancer uniquement l’application Streamlit

```bash
make run-app
```

### Vérifier l’état des conteneurs

```bash
make check-airflow
```

---

## Détails techniques

* **Airflow** : version 3.1.0 (orchestrateur ETL)
* **Python** : 3.13
* **Docker Compose** : gère les services Airflow, Postgres, Redis, PgAdmin, Streamlit
* **Volumes persistants** :

  * `/data` : fichiers bruts, staging et curated
  * `/logs` : journaux d’exécution Airflow

---

## Bonnes pratiques

* Toujours exécuter `make stop-with-volumes` **avant une réinitialisation complète** d’Airflow.
* Les DAGs sont situés dans `src/dags/` et montés automatiquement dans le conteneur Airflow.
* Pour ajouter un nouveau DAG, dépose ton fichier Python dans `src/dags/` puis redémarre le service Airflow.

---

## Exemple de workflow Airflow

1. **Extraction (Ingestion)** : télécharge ou lit les données sources.
2. **Transformation** : nettoie, filtre et structure les données.
3. **Chargement (Load)** : insère les résultats dans PostgreSQL.
4. **Visualisation** : Streamlit lit directement les tables pour affichage interactif.

---

## Auteurs

* **Junior**, **Nihal**, **Zineb**


### Abstract

## Datasets Description 

## Queries 

## Requirements

## Note for Students

* Clone the created repository offline;
* Add your name and surname into the Readme file and your teammates as collaborators
* Complete the field above after project is approved
* Make any changes to your repository according to the specific assignment;
* Ensure code reproducibility and instructions on how to replicate the results;
* Add an open-source license, e.g., Apache 2.0;
* README is automatically converted into pdf

