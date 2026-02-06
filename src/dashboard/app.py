"""Streamlit dashboard with interactive crime heatmap for the Netherlands."""

import json
import sys
from pathlib import Path

# Ensure project root is on the Python path
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import folium
import pandas as pd
import plotly.express as px
import streamlit as st
from sqlalchemy import text
from streamlit_folium import st_folium

from src.database.connection import engine

DATA_DIR = Path(__file__).resolve().parents[2] / "data" / "raw"

st.set_page_config(
    page_title="Netherlands Crime Dashboard",
    page_icon=":bar_chart:",
    layout="wide",
)


@st.cache_data(ttl=600)
def load_crime_data() -> pd.DataFrame:
    """Load aggregated crime data from the database."""
    query = text("""
        SELECT
            r.region_code,
            r.region_name,
            ct.crime_code,
            ct.crime_name,
            p.year,
            f.registered_crimes,
            f.registered_crimes_per_1000
        FROM fact_crimes f
        JOIN dim_regions r ON f.region_id = r.id
        JOIN dim_crime_types ct ON f.crime_type_id = ct.id
        JOIN dim_periods p ON f.period_id = p.id
    """)
    with engine.connect() as conn:
        df = pd.read_sql(query, conn)
    return df


@st.cache_data(ttl=600)
def load_geojson() -> dict:
    """Load municipality GeoJSON boundaries."""
    geo_path = DATA_DIR / "municipalities.geojson"
    with open(geo_path, encoding="utf-8") as f:
        return json.load(f)


def get_municipality_code_field(geojson: dict) -> str:
    """Detect the GeoJSON property field that contains the municipality code."""
    if not geojson.get("features"):
        return "identificatie"
    props = geojson["features"][0].get("properties", {})
    for candidate in ["identificatie", "gemeentecode", "code", "GM_CODE", "statcode"]:
        if candidate in props:
            return candidate
    return list(props.keys())[0]


def build_choropleth(
    geojson: dict,
    map_data: pd.DataFrame,
    code_field: str,
    value_col: str,
    legend_name: str,
) -> folium.Map:
    """Build a Folium choropleth map of the Netherlands."""
    m = folium.Map(location=[52.2, 5.3], zoom_start=7, tiles="cartodbpositron")

    folium.Choropleth(
        geo_data=geojson,
        name="choropleth",
        data=map_data,
        columns=["region_code", value_col],
        key_on=f"feature.properties.{code_field}",
        fill_color="YlOrRd",
        fill_opacity=0.7,
        line_opacity=0.3,
        legend_name=legend_name,
        nan_fill_color="white",
    ).add_to(m)

    folium.LayerControl().add_to(m)
    return m


def main() -> None:
    st.title("Netherlands Crime Dashboard")
    st.markdown("Interactive visualization of registered crime across Dutch municipalities.")

    # --- Load data with spinner ---
    with st.spinner("Loading crime data from database..."):
        try:
            df = load_crime_data()
        except Exception as e:
            st.error(f"Failed to load crime data: {e}")
            st.info("Make sure the pipeline has been run and the database is available.")
            return

    with st.spinner("Loading municipality boundaries..."):
        try:
            geojson = load_geojson()
        except Exception as e:
            st.error(f"Failed to load GeoJSON: {e}")
            return

    if df.empty:
        st.warning("No crime data found. Run the pipeline first.")
        return

    # --- Sidebar filters ---
    st.sidebar.header("Filters")

    years = sorted(df["year"].unique())
    selected_year = st.sidebar.selectbox("Year", years, index=len(years) - 1, key="year_filter")

    crime_types = sorted(df["crime_name"].unique())
    selected_crime = st.sidebar.selectbox("Crime Type", ["All"] + crime_types, key="crime_filter")

    # --- Show active selection ---
    crime_label = "All Crime Types" if selected_crime == "All" else selected_crime
    st.info(f"Showing: **{crime_label}** in **{selected_year}**")

    # --- Filter data with spinner ---
    with st.spinner("Filtering and aggregating data..."):
        filtered = df[df["year"] == selected_year].copy()
        if selected_crime != "All":
            filtered = filtered[filtered["crime_name"] == selected_crime]

        # Aggregate by municipality (sum across crime types if "All")
        agg = (
            filtered.groupby(["region_code", "region_name"])
            .agg(
                total_crimes=("registered_crimes", "sum"),
                avg_rate_per_1000=("registered_crimes_per_1000", "mean"),
            )
            .reset_index()
        )

    # --- Summary cards ---
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Total Registered Crimes", f"{agg['total_crimes'].sum():,.0f}")
    with col2:
        if not agg.empty:
            top = agg.loc[agg["total_crimes"].idxmax()]
            st.metric("Highest Crime Municipality", top["region_name"])
        else:
            st.metric("Highest Crime Municipality", "N/A")
    with col3:
        st.metric("Municipalities", f"{len(agg)}")

    # --- Map ---
    st.subheader(f"Crime Heatmap - {crime_label} ({selected_year})")

    with st.spinner("Building map..."):
        code_field = get_municipality_code_field(geojson)

        # Match GeoJSON codes to CBS codes
        if geojson.get("features"):
            sample_code = str(
                geojson["features"][0].get("properties", {}).get(code_field, "")
            )
            if not sample_code.startswith("GM") and agg["region_code"].str.startswith("GM").all():
                agg["region_code"] = agg["region_code"].str.replace("GM", "", n=1)

        m = build_choropleth(
            geojson=geojson,
            map_data=agg,
            code_field=code_field,
            value_col="total_crimes",
            legend_name=f"Registered Crimes - {crime_label} ({selected_year})",
        )

    st_folium(m, width=900, height=600, returned_objects=[])

    # --- Top 10 bar chart ---
    st.subheader(f"Top 10 Municipalities - {crime_label} ({selected_year})")
    top10 = agg.nlargest(10, "total_crimes")
    fig_bar = px.bar(
        top10,
        x="region_name",
        y="total_crimes",
        color="total_crimes",
        color_continuous_scale="OrRd",
        labels={"region_name": "Municipality", "total_crimes": "Registered Crimes"},
    )
    fig_bar.update_layout(showlegend=False, xaxis_tickangle=-45)
    st.plotly_chart(fig_bar, use_container_width=True)

    # --- National trend line ---
    trend_title = f"National Trend - {crime_label}"
    st.subheader(trend_title)
    trend_df = df.copy()
    if selected_crime != "All":
        trend_df = trend_df[trend_df["crime_name"] == selected_crime]
    yearly = (
        trend_df.groupby("year")
        .agg(total_crimes=("registered_crimes", "sum"))
        .reset_index()
    )
    fig_line = px.line(
        yearly,
        x="year",
        y="total_crimes",
        markers=True,
        labels={"year": "Year", "total_crimes": "Total Registered Crimes"},
    )
    st.plotly_chart(fig_line, use_container_width=True)

    # --- Data table ---
    with st.expander(f"View Raw Data - {crime_label} ({selected_year})"):
        st.dataframe(agg.sort_values("total_crimes", ascending=False), use_container_width=True)


if __name__ == "__main__":
    main()
