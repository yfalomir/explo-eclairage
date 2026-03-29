from pathlib import Path

import geopandas as gpd
import polars as pl

from explo_eclairage import connector, transform


def run(
    crime_path: Path,
    extinction_path: Path,
    output_gpkg_path: Path,
    window: int = 2,
) -> gpd.GeoDataFrame:
    
    crime = connector.load_crime_data(crime_path)
    extinction = connector.load_extinction_data(extinction_path)

    extinction = transform.extract_extinction_year(extinction)
    crime = transform.compute_crime_delta(crime, window=window)
    extinction_crime = transform.join_extinction(crime, extinction)
    extinction_crime = transform.compute_dep_baseline(extinction_crime, window)
    cities = transform.aggregate_city_share(extinction_crime)
    cities = transform.filter_extinction_years(cities, window=window)
    stats_category_pop, global_stats = transform.compute_extinction_stats(cities)


    with pl.Config(tbl_rows=100, tbl_cols=-1) as cfg:
        cfg.set_tbl_formatting('ASCII_MARKDOWN')
        print(stats_category_pop.filter(pl.col("annee").is_between(2017, 2023)))

    with pl.Config(tbl_rows=100, tbl_cols=-1) as cfg:
        cfg.set_tbl_formatting('ASCII_MARKDOWN')
        print(global_stats.filter(pl.col("annee").is_between(2017, 2023)))


    transform.plot_extinction_diff(stats_category_pop.filter(pl.col("annee").is_between(2018, 2022)), output_gpkg_path.parent / "median_ratio_diff_by_population.png")
    transform.plot_global_stats(global_stats.filter(pl.col("annee").is_between(2018, 2022)), output_gpkg_path.parent)
    transform.plot_global_stats_boxplot(cities.filter(pl.col("annee").is_between(2018, 2022)), output_gpkg_path.parent)

    print(cities.filter(pl.col("extinction") & pl.col("crime_data_available"))["CODGEO_2025"].n_unique())

    gdf = connector.to_geodataframe(cities)
    connector.save_results(gdf, output_gpkg_path)
    return gdf



if __name__ == "__main__":

    run(
        crime_path=Path("data/donnee-comm-data.gouv-parquet-2024-geographie2025-produit-le2025-06-04.parquet"),
        extinction_path=Path("data/vectorExtinctionFrance.gpkg"),
        output_gpkg_path=Path("export/cities_crime_increase_ratio.gpkg"),
    )
