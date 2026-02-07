"""Streamlit dashboard with interactive crime heatmap for the Netherlands."""

import json
import sys
from pathlib import Path

# Ensure project root is on the Python path
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from sqlalchemy import text

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
    """Load simplified municipality GeoJSON boundaries."""
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


def ensure_all_municipalities(
    geojson: dict, agg: pd.DataFrame, code_field: str, value_col: str,
) -> pd.DataFrame:
    """Left join: ensure every GeoJSON municipality appears in the data."""
    geo_codes = []
    geo_names = []
    for feat in geojson.get("features", []):
        props = feat.get("properties", {})
        geo_codes.append(props.get(code_field, ""))
        geo_names.append(props.get("naam", props.get(code_field, "")))

    all_munis = pd.DataFrame({"region_code": geo_codes, "geo_name": geo_names})
    merged = all_munis.merge(agg, on="region_code", how="left")

    # Fill missing region names from GeoJSON
    merged["region_name"] = merged["region_name"].fillna(merged["geo_name"])
    merged["has_data"] = merged[value_col].notna()
    merged = merged.drop(columns=["geo_name"])

    return merged


def build_choropleth(
    geojson: dict,
    map_data: pd.DataFrame,
    code_field: str,
    value_col: str,
    metric_label: str,
) -> go.Figure:
    """Build a Plotly choropleth map of the Netherlands."""
    has_data = map_data[map_data["has_data"]].copy()
    no_data = map_data[~map_data["has_data"]].copy()

    fig = px.choropleth_mapbox(
        has_data,
        geojson=geojson,
        locations="region_code",
        featureidkey=f"properties.{code_field}",
        color=value_col,
        color_continuous_scale="OrRd",
        hover_name="region_name",
        hover_data={
            "region_code": False,
            value_col: ":.1f",
        },
        labels={value_col: metric_label},
        mapbox_style="carto-positron",
        center={"lat": 52.2, "lon": 5.3},
        zoom=6.3,
        opacity=0.7,
    )

    if not no_data.empty:
        no_data[value_col] = 0
        fig_gray = px.choropleth_mapbox(
            no_data,
            geojson=geojson,
            locations="region_code",
            featureidkey=f"properties.{code_field}",
            color_discrete_sequence=["#d3d3d3"],
            hover_name="region_name",
            hover_data={"region_code": False},
            mapbox_style="carto-positron",
            center={"lat": 52.2, "lon": 5.3},
            zoom=6.3,
            opacity=0.5,
        )
        for trace in fig_gray.data:
            trace.hovertemplate = "<b>%{hovertext}</b><br>No data available<extra></extra>"
            trace.showlegend = False
            fig.add_trace(trace)

    fig.update_layout(
        margin={"r": 0, "t": 0, "l": 0, "b": 0},
        height=600,
    )
    return fig


@st.cache_data(ttl=600)
def filter_and_aggregate(
    _df: pd.DataFrame, selected_year: int, selected_crime: str,
) -> pd.DataFrame:
    """Cache filtered + aggregated data per filter combination."""
    filtered = _df[_df["year"] == selected_year].copy()
    if selected_crime != "All":
        filtered = filtered[filtered["crime_name"] == selected_crime]

    agg = (
        filtered.groupby(["region_code", "region_name"])
        .agg(
            total_crimes=("registered_crimes", "sum"),
            avg_rate_per_1000=("registered_crimes_per_1000", "mean"),
        )
        .reset_index()
    )
    return agg


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

    st.sidebar.divider()
    metric_options = {
        "Total Crimes": "total_crimes",
        "Crime Rate per 1,000 inhabitants": "avg_rate_per_1000",
    }
    selected_metric_label = st.sidebar.radio(
        "Map Metric",
        list(metric_options.keys()),
        index=1,
        key="metric_filter",
        help="'Per 1,000' normalizes by population so small and large cities are comparable",
    )
    selected_metric = metric_options[selected_metric_label]

    # --- Show active selection ---
    crime_label = "All Crime Types" if selected_crime == "All" else selected_crime
    st.info(f"Showing: **{crime_label}** in **{selected_year}** | Metric: **{selected_metric_label}**")

    # --- Cached aggregation (instant on repeat filter) ---
    agg = filter_and_aggregate(df, selected_year, selected_crime)

    code_field = get_municipality_code_field(geojson)

    # Match GeoJSON codes to CBS codes
    if geojson.get("features"):
        sample_code = str(
            geojson["features"][0].get("properties", {}).get(code_field, "")
        )
        if not sample_code.startswith("GM") and agg["region_code"].str.startswith("GM").all():
            agg["region_code"] = agg["region_code"].str.replace("GM", "", n=1)

    agg = ensure_all_municipalities(geojson, agg, code_field, selected_metric)

    # --- Summary cards ---
    agg_with_data = agg[agg["has_data"]]
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Total Registered Crimes", f"{agg_with_data['total_crimes'].sum():,.0f}")
    with col2:
        if not agg_with_data.empty:
            st.metric("Avg Rate per 1,000", f"{agg_with_data['avg_rate_per_1000'].mean():,.1f}")
        else:
            st.metric("Avg Rate per 1,000", "N/A")
    with col3:
        if not agg_with_data.empty:
            top = agg_with_data.loc[agg_with_data[selected_metric].idxmax()]
            st.metric("Highest Municipality", top["region_name"])
        else:
            st.metric("Highest Municipality", "N/A")
    with col4:
        has_count = agg["has_data"].sum()
        st.metric("Municipalities", f"{has_count} / {len(agg)}")

    # --- Map and charts in a fragment (only this section re-renders) ---
    @st.fragment
    def render_visualizations():
        st.subheader(f"Crime Heatmap - {crime_label} ({selected_year})")
        fig_map = build_choropleth(
            geojson=geojson,
            map_data=agg,
            code_field=code_field,
            value_col=selected_metric,
            metric_label=selected_metric_label,
        )
        st.plotly_chart(fig_map, use_container_width=True)

        st.subheader(f"Top 10 Municipalities by {selected_metric_label} - {crime_label} ({selected_year})")
        top10 = agg_with_data.nlargest(10, selected_metric)
        fig_bar = px.bar(
            top10,
            x="region_name",
            y=selected_metric,
            color=selected_metric,
            color_continuous_scale="OrRd",
            labels={"region_name": "Municipality", selected_metric: selected_metric_label},
        )
        fig_bar.update_layout(showlegend=False, xaxis_tickangle=-45)
        st.plotly_chart(fig_bar, use_container_width=True)

        st.subheader(f"National Trend - {crime_label}")
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

        with st.expander(f"View Raw Data - {crime_label} ({selected_year})"):
            st.dataframe(
                agg_with_data.sort_values("total_crimes", ascending=False),
                use_container_width=True,
            )

    render_visualizations()


if __name__ == "__main__":
    main()
