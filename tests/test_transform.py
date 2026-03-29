import polars as pl
import pytest

from explo_eclairage import transform


# --- extract_extinction_year ---


def test_extract_extinction_year_normal(extinction_df):
    result = transform.extract_extinction_year(extinction_df)
    assert result["extinction_year"][0] == 2020


def test_extract_extinction_year_null():
    df = pl.DataFrame({"Date Extinction EP": [None]})
    result = transform.extract_extinction_year(df)
    assert result["extinction_year"][0] is None


def test_extract_extinction_year_malformed():
    df = pl.DataFrame({"Date Extinction EP": ["not-a-date"]})
    result = transform.extract_extinction_year(df)
    assert result["extinction_year"][0] is None


# --- compute_crime_delta ---


def test_compute_crime_delta_city_doubles(crime_df):
    result = transform.compute_crime_delta(crime_df, window=2)
    # City A at 2020: after=(20+20), before=(10+10) → delta=20
    row = result.filter((pl.col("CODGEO_2025") == "A") & (pl.col("annee") == 2020))
    assert row["delta_2_2_years"][0] == 20.0


def test_compute_crime_delta_flat_city(crime_df):
    result = transform.compute_crime_delta(crime_df, window=2)
    # City B is flat → delta=0
    row = result.filter((pl.col("CODGEO_2025") == "B") & (pl.col("annee") == 2020))
    assert row["delta_2_2_years"][0] == 0.0


def test_compute_crime_delta_boundary_rows_are_null(crime_df):
    result = transform.compute_crime_delta(crime_df, window=2)
    # First two rows of each city lack enough history → delta should be null
    row = result.filter((pl.col("CODGEO_2025") == "A") & (pl.col("annee") == 2018))
    assert row["delta_2_2_years"][0] is None


def test_compute_crime_delta_increase_ratio(crime_df):
    result = transform.compute_crime_delta(crime_df, window=2)
    # City A at 2020: delta=20, nombre=10 → ratio=2.0
    row = result.filter((pl.col("CODGEO_2025") == "A") & (pl.col("annee") == 2020))
    assert row["increase_ratio_2_2_years"][0] == pytest.approx(2.0)


# --- join_extinction ---


def test_join_extinction_matched(crime_df, extinction_df):
    extinction_df = transform.extract_extinction_year(extinction_df)
    crime_with_delta = transform.compute_crime_delta(crime_df)
    result = transform.join_extinction(crime_with_delta, extinction_df)

    matched = result.filter(pl.col("CODGEO_2025") == "A")
    assert (matched["extinction_year"] == 2020).all()


def test_join_extinction_unmatched_is_null(crime_df, extinction_df):
    extinction_df = transform.extract_extinction_year(extinction_df)
    crime_with_delta = transform.compute_crime_delta(crime_df)
    result = transform.join_extinction(crime_with_delta, extinction_df)

    unmatched = result.filter(pl.col("CODGEO_2025") == "B")
    assert unmatched["extinction_year"].is_null().all()


# --- compute_dep_baseline ---


def test_compute_dep_baseline_growing_city_is_above(crime_df, extinction_df):
    extinction_df = transform.extract_extinction_year(extinction_df)
    crime_with_delta = transform.compute_crime_delta(crime_df)
    joined = transform.join_extinction(crime_with_delta, extinction_df)
    result = transform.compute_dep_baseline(joined)

    # City A grew, city B flat → dept average is positive → A should be above
    row = result.filter((pl.col("CODGEO_2025") == "A") & (pl.col("annee") == 2020))
    assert row["above_dep_ratio"][0] is True


def test_compute_dep_baseline_flat_city_is_below(crime_df, extinction_df):
    extinction_df = transform.extract_extinction_year(extinction_df)
    crime_with_delta = transform.compute_crime_delta(crime_df)
    joined = transform.join_extinction(crime_with_delta, extinction_df)
    result = transform.compute_dep_baseline(joined)

    row = result.filter((pl.col("CODGEO_2025") == "B") & (pl.col("annee") == 2020))
    assert row["above_dep_ratio"][0] is False
