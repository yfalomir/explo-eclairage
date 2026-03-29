.PHONY: data run

CRIME_FILE    := data/donnee-comm-data.gouv-parquet-2024-geographie2025-produit-le2025-06-04.parquet
EXTINCTION_FILE := data/vectorExtinctionFrance.gpkg

data: $(CRIME_FILE) $(EXTINCTION_FILE)

$(CRIME_FILE):
	@mkdir -p data
	@echo "Download the crime data (Parquet, commune level, géographie 2025) from:"
	@echo "  https://www.data.gouv.fr/datasets/bases-statistiques-communale-departementale-et-regionale-de-la-delinquance-enregistree-par-la-police-et-la-gendarmerie-nationales"
	@echo "and save it to $@"

$(EXTINCTION_FILE):
	@mkdir -p data
	@echo "Download the public lighting extinction GeoPackage from:"
	@echo "  https://www.data.gouv.fr/datasets/cartographie-nationale-des-pratiques-declairage-nocturne"
	@echo "and save it to $@"

run:
	uv run python src/explo_eclairage/pipeline.py
