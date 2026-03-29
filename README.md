# explo-eclairage

Exploration of the relationship between public lighting extinction and crime in French municipalities.

## Setup

```bash
uv sync
```

## Download source data

Two datasets are required and must be placed in the `data/` directory.

### 1. Crime statistics (commune level)

Source: [Bases statistiques communale, départementale et régionale de la délinquance — data.gouv.fr](https://www.data.gouv.fr/datasets/bases-statistiques-communale-departementale-et-regionale-de-la-delinquance-enregistree-par-la-police-et-la-gendarmerie-nationales)

Download the **Parquet (commune level, géographie 2025)** file and save it as:

```
data/donnee-comm-data.gouv-parquet-2024-geographie2025-produit-le2025-06-04.parquet
```

### 2. Public lighting extinction vector data

Source: [Cartographie nationale des pratiques d'éclairage nocturne — data.gouv.fr](https://www.data.gouv.fr/datasets/cartographie-nationale-des-pratiques-declairage-nocturne)

Download the GeoPackage file and save it as:

```
data/vectorExtinctionFrance.gpkg
```

Run `make data` to print the download instructions for both files.

## Run

```bash
make run
```

Results are written to `export/`.
