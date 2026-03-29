from pathlib import Path

import polars as pl
import matplotlib.pyplot as plt
from matplotlib.ticker import MaxNLocator


def extract_extinction_year(df: pl.DataFrame) -> pl.DataFrame:
    """Parse 'Date Extinction EP' raw string to an Int32 year.
    Represents the first year of extinction

    Raw format example: "['2019-03']" or "['2019-03', '2022-12']" 
    """
    return df.with_columns(
        extinction_year=pl.col("Date Extinction EP")
        .str.extract(r"'(\d{4})-", 1)
        .cast(pl.Int32)
    )


def compute_crime_delta(df: pl.DataFrame, window: int = 2) -> pl.DataFrame:
    """For each (city, indicator), compute the sum of crimes in the `window`
    years after minus the sum in the `window` years before the reference year.

    Adds columns:
      - delta_{window}_{window}_years: absolute difference (after - before)
      - increase_ratio_{window}_{window}_years: delta relative to the reference year count
    """
    partition = ["CODGEO_2025", "indicateur"]
    delta_col = f"delta_{window}_{window}_years"
    ratio_col = f"increase_ratio_{window}_{window}_years"

    shifts_after = [
        pl.col("nombre").shift(-i).over(partition, order_by="annee")
        for i in range(1, window + 1)
    ]
    shifts_before = [
        pl.col("nombre").shift(i).over(partition, order_by="annee")
        for i in range(1, window + 1)
    ]

    delta = sum(shifts_after) - sum(shifts_before)

    return df.with_columns(delta.alias(delta_col)).with_columns(
        (pl.col(delta_col) / pl.max_horizontal(1, pl.col("nombre"))).alias(ratio_col)
    )


def join_extinction(
    crime_df: pl.DataFrame,
    extinction_df: pl.DataFrame,
) -> pl.DataFrame:
    """Left-join crime evolution data with extinction city data."""
    return crime_df.join(
        extinction_df,
        left_on="CODGEO_2025",
        right_on="insee_com",
        how="left",
    )


def compute_dep_baseline(df: pl.DataFrame, window: int) -> pl.DataFrame:
    """For each (dept, indicator, year), compute the departmental increase
    ratio, then flag rows where the city grew faster than its department.

    Adds columns:
      - dep_increase_ratio_{window}_{window}_years: dept-level ratio
      - above_dep_ratio: True if city ratio >= dept ratio
    """
    group = ["insee_dep", "indicateur", "annee"]
    return df.with_columns(
        (
            pl.col(f"delta_{window}_{window}_years").sum().over(group)
            / pl.max_horizontal(1, pl.col("nombre").sum().over(group))
        ).alias(f"dep_increase_ratio_{window}_{window}_years")
    ).with_columns(
        (pl.col(f"increase_ratio_{window}_{window}_years") >= pl.col(f"dep_increase_ratio_{window}_{window}_years")).alias(
            "above_dep_ratio"
        )
    )




def aggregate_city_share(df: pl.DataFrame) -> pl.DataFrame:
    """For each city, at its extinction year, compute the share of crime
    indicators that grew faster than the department average.

    Adds columns:
      - extinction: True if the city has an extinction year in the data
      - n_indicator_above_dep: count of indicators above dept trend at extinction year
      - n_indicator: total count of indicators at extinction year
      - crime_data_available: True if n_indicator > 0
      - ratio: n_indicator_above_dep / n_indicator
    """
    at_extinction = (pl.col("annee") == pl.col("extinction_year")).any()

    cities = df.filter(pl.col("geometry").is_not_null()).group_by(
        ["CODGEO_2025", "annee", "nom", "insee_dep", "geometry", "insee_pop"]
    ).agg(
        pl.when(at_extinction).then(True).otherwise(False).alias("extinction"),
        pl.col("above_dep_ratio")
        .cast(pl.Float64)
        .sum()
        .alias("n_indicator_above_dep"),
        pl.col("above_dep_ratio")
        .count()
        .alias("n_indicator"),
    )

    return cities.with_columns(
        (pl.col("n_indicator") > 0).alias("crime_data_available")
    ).with_columns(
        (pl.col("n_indicator_above_dep") / pl.max_horizontal(1, pl.col("n_indicator"))).alias(
            "ratio"
        )
    )

def filter_extinction_years(df: pl.DataFrame, window: int = 2) -> pl.DataFrame:
    """For cities with an extinction year, keep only rows strictly before
    (extinction_year - window). Cities with no extinction rows are kept as-is.
    """
    has_extinction = pl.col("extinction").any().over("CODGEO_2025")
    extinction_year = (
        pl.when(pl.col("extinction"))
        .then(pl.col("annee"))
        .otherwise(None)
        .min()
        .over("CODGEO_2025")
    )
    return df.filter(
        ~has_extinction | (pl.col("annee") < extinction_year - window) | pl.col("extinction")
    )


def compute_extinction_stats(df: pl.DataFrame) -> pl.DataFrame:
    """Compare crime ratio statistics between extinct and non-extinct cities,
    matched by year to avoid temporal confounding.

    Returns one row per (annee, extinction, population_category) with:
      - n_cities: number of cities in the group
      - mean_ratio: average share of indicators above dept trend
      - q25_ratio, median_ratio, q75_ratio: distribution of that share
      - pct_majority_above: share of cities where >50% of indicators grew faster than dept
    """
    return (
        df.with_columns(
            pl.col("insee_pop")
            .cut([4000], labels=["<4000", ">4000"])
            .alias("population_category")
        )
        .group_by(["annee", "extinction", "population_category"])
        .agg(
            pl.len().alias("n_cities"),
            pl.col("ratio").mean().alias("mean_ratio"),
            pl.col("ratio").quantile(0.25).alias("q25_ratio"),
            pl.col("ratio").median().alias("median_ratio"),
            pl.col("ratio").quantile(0.75).alias("q75_ratio"),
            (pl.col("ratio") > 0.5).mean().alias("pct_majority_above"),
        )
        .sort(["annee", "population_category", "extinction"]),
        df.group_by(["annee", "extinction"])
        .agg(
            pl.len().alias("n_cities"),
            pl.col("ratio").mean().alias("mean_ratio"),
            pl.col("ratio").quantile(0.25).alias("q25_ratio"),
            pl.col("ratio").median().alias("median_ratio"),
            pl.col("ratio").quantile(0.75).alias("q75_ratio"),
            (pl.col("ratio") > 0.5).mean().alias("pct_majority_above"),
        )
        .sort(["annee", "extinction"]),

    )

def plot_extinction_diff(stats: pl.DataFrame, output_path: Path) -> None:
    """Plot median_ratio(extinction=True) - median_ratio(extinction=False) over years,
    one line per population_category."""
    extinct = stats.filter(pl.col("extinction")).select(
        ["annee", "population_category", pl.col("median_ratio").alias("median_ratio_ext")]
    )
    non_extinct = stats.filter(~pl.col("extinction")).select(
        ["annee", "population_category", pl.col("median_ratio").alias("median_ratio_non_ext")]
    )
    diff = extinct.join(non_extinct, on=["annee", "population_category"]).with_columns(
        (pl.col("median_ratio_ext") - pl.col("median_ratio_non_ext")).alias("diff")
    )

    categories = diff["population_category"].unique().sort().to_list()
    fig, ax = plt.subplots(figsize=(10, 6))
    for cat in categories:
        subset = diff.filter(pl.col("population_category") == cat).sort("annee")
        ax.plot(subset["annee"].to_list(), subset["diff"].to_list(), marker="o", label=cat)

    ax.axhline(0, color="black", linewidth=0.8, linestyle="--")
    ax.xaxis.set_major_locator(MaxNLocator(integer=True))
    ax.set_xlabel("Année")
    ax.set_ylabel("Différence median_ratio (extinction − non-extinction)")
    ax.set_title("Écart de ratio de criminalité entre villes éteintes et non-éteintes")
    ax.legend(title="Catégorie de population")
    fig.tight_layout()
    fig.text(0.99, 0.01, "Lorem ipsum", ha="right", va="bottom", fontsize=8, color="gray")
    fig.savefig(output_path, dpi=150)
    plt.close(fig)
    print(f"Plot saved to {output_path}")

def plot_global_stats_boxplot(cities: pl.DataFrame, output_dir: Path) -> None:
    """Boxplot of ratio distribution per year, split by extinction status."""
    output_path = output_dir / "global_stats_boxplot.png"
    years = sorted(cities["annee"].unique().to_list())
    fig, ax = plt.subplots(figsize=(12, 6))

    positions_ext = [i - 0.2 for i in range(len(years))]
    positions_non = [i + 0.2 for i in range(len(years))]

    data_ext = [
        cities.filter((pl.col("annee") == y) & pl.col("extinction"))["ratio"].to_list()
        for y in years
    ]
    data_non = [
        cities.filter((pl.col("annee") == y) & ~pl.col("extinction"))["ratio"].to_list()
        for y in years
    ]

    bp_ext = ax.boxplot(data_ext, positions=positions_ext, widths=0.35, patch_artist=True,
                        boxprops=dict(facecolor="steelblue", alpha=0.7),
                        medianprops=dict(color="white"), showfliers=False)
    bp_non = ax.boxplot(data_non, positions=positions_non, widths=0.35, patch_artist=True,
                        boxprops=dict(facecolor="darkorange", alpha=0.7),
                        medianprops=dict(color="white"), showfliers=False)

    ax.set_xticks(range(len(years)))
    ax.set_xticklabels(years)
    ax.set_xlabel("Année")
    ax.set_ylabel("Proportion des indicateurs*")
    ax.set_title("Distribution du ratio de criminalité par année (extinction vs non-extinction)")
    ax.legend([bp_ext["boxes"][0], bp_non["boxes"][0]], ["Extinction", "Sans extinction"])
    fig.tight_layout()
    fig.text(0.99, 0.01, "*proportion des indicateurs en augmentation plus rapide dans la commune que dans le reste du département",
             ha="right", va="bottom", fontsize=8, color="gray")
    fig.savefig(output_path, dpi=150)
    plt.close(fig)
    print(f"Plot saved to {output_path}")


def plot_global_stats(stats: pl.DataFrame, output_dir: Path) -> None:
    """Plot median_ratio and mean_ratio over years, one line for extinction=True and one for extinction=False."""
    for metric in ["median_ratio", "mean_ratio"]:
        output_path = output_dir / f"global_stats_{metric}.png"
        fig, ax = plt.subplots(figsize=(10, 6))
        for is_extinct in [True, False]:
            subset = (
                stats.filter(pl.col("extinction") == is_extinct)
                .select(["annee", metric])
                .sort("annee")
            )
            label = "Extinction" if is_extinct else "Sans extinction"
            ax.plot(subset["annee"].to_list(), subset[metric].to_list(), marker="o", label=label)

        ax.xaxis.set_major_locator(MaxNLocator(integer=True))
        ax.set_xlabel("Année")
        ax.set_ylabel({
            "mean_ratio": "Proportion moyenne des indicateurs",
            "median_ratio": "Proportion médiane des indicateurs"
        }[metric])
        ax.set_title(
            {
            "mean_ratio": "Évolution moyenne des indicateurs* de criminalité par année d'extinction",
            "median_ratio": "Évolution médiane des indicateurs* de criminalité par année d'extinction",
        }[metric])
        ax.legend()
        fig.tight_layout()
        fig.text(0.99, 0.01, "*proportion des indicateurs en augmentation plus rapide dans la commune que dans le reste du département"
                , ha="right", va="bottom", fontsize=8, color="gray")
        fig.savefig(output_path, dpi=150)

        plt.close(fig)
        print(f"Plot saved to {output_path}")
