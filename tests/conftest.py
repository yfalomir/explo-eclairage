import polars as pl
import pytest


@pytest.fixture
def crime_df():
    """Two cities, one indicator, 6 years. City A doubles after 2020, B is flat."""
    return pl.DataFrame(
        {
            "CODGEO_2025": ["A"] * 6 + ["B"] * 6,
            "indicateur": ["vol"] * 12,
            "annee": list(range(2018, 2024)) * 2,
            "nombre": [10, 10, 10, 20, 20, 20, 5, 5, 5, 5, 5, 5],
            "insee_dep": ["01"] * 12,
        }
    )


@pytest.fixture
def extinction_df():
    """One city with an extinction date."""
    return pl.DataFrame(
        {
            "insee_com": ["A"],
            "nom": ["Commune A"],
            "insee_dep": ["01"],
            "Date Extinction EP": ["['2020-03']"],
            "geometry": [None],
        }
    )
