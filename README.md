Assistant de recherche orienté services publics marocains, basé sur un pipeline de collecte, structuration, segmentation et indexation de contenus web.

## Objectif

Ce projet prépare une base de connaissances exploitable pour un assistant en darija autour de démarches et services publics au Maroc.

Le pipeline actuel permet de :

- scraper des pages sources depuis une liste d'URLs ;
- extraire et structurer le contenu utile ;
- construire un `dataset.json` consolidé ;
- découper le contenu en chunks ;
- générer des embeddings et un index FAISS pour la recherche sémantique.

## Structure

scripts/              Scripts du pipeline
data/raw_html/        HTML bruts collectés
data/structured/      Données nettoyées et structurées
data/chunks/          Chunks prêts pour l'indexation
data/reports/         Rapports d'exécution
embeddings/           Index FAISS et enregistrements vectorisés
dataset.json          Dataset consolidé final
urls.json             Liste des URLs à traiter
```

## Scripts principaux

- `scripts/scrape.py` : télécharge les pages et extrait le contenu utile.
- `scripts/build_dataset.py` : fusionne les enregistrements structurés dans `dataset.json`.
- `scripts/chunk.py` : segmente le dataset en chunks exploitables pour la recherche.
- `scripts/embed.py` : génère les embeddings et construit l'index FAISS.
- `scripts/test_query.py` : test local simple de recherche vectorielle.

## Prérequis

- Python 3
- environnement virtuel avec les dépendances nécessaires
- accès réseau pour le scraping et, si besoin, le téléchargement des modèles

## Utilisation

Exemples avec l'environnement virtuel du projet :

```powershell
.\venv\Scripts\python.exe scripts\scrape.py
.\venv\Scripts\python.exe scripts\build_dataset.py
.\venv\Scripts\python.exe scripts\chunk.py
.\venv\Scripts\python.exe scripts\embed.py
.\venv\Scripts\python.exe scripts\test_query.py
```

Pour forcer l'utilisation d'un modèle déjà présent en cache local :

```powershell
.\venv\Scripts\python.exe scripts\embed.py --local-only
```

## Sorties importantes

- `dataset.json` : base consolidée des contenus retenus
- `data/chunks/chunks.json` : segments texte prêts pour l'embedding
- `embeddings/index.faiss` : index vectoriel
- `embeddings/records.jsonl` : chunks avec métadonnées et vecteurs

## État actuel

Le dépôt contient déjà des données collectées, un dataset consolidé, des chunks, ainsi qu'un index d'embeddings prêt à être exploité.
