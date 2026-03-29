from pathlib import Path

import geopandas as gpd
import geopolars
import polars as pl


def load_crime_data(path: Path) -> pl.DataFrame:
    return pl.read_parquet(path)


def load_extinction_data(path: Path) -> pl.DataFrame:
    return geopolars.read_file(path, layer="extinction_communes")[
        ["insee_com", "nom", "insee_dep", "Date Extinction EP", "geometry"]
    ]


def to_geodataframe(df: pl.DataFrame) -> gpd.GeoDataFrame:
    return gpd.GeoDataFrame(
        df.drop("geometry").to_pandas(),
        geometry=gpd.GeoSeries.from_wkb(df["geometry"]),
        crs="EPSG:4326",
    )


def save_results(gdf: gpd.GeoDataFrame, path: Path) -> None:
    gdf.to_file(path, driver="GPKG")
