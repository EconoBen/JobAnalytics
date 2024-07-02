import plotly.express as px
import polars as pl
import streamlit as st
from polars import DataFrame


def load_and_process_data() -> DataFrame:
    raw_data = pl.read_csv("../data/WildRP Job Data - Raw Data.csv")

    value_data = (
        pl.read_csv("../data/WildRP_job_data_values_only.csv")
        .rename({"": "JOB"})
        .filter(pl.col("JOB").is_not_null())
        .select(pl.col(["JobTemplateId", "JOB"]))
    )

    joined_data = value_data.join(raw_data, on="JobTemplateId", how="inner")

    return joined_data


def create_leaderboard_plot(data: DataFrame):
    df = data.to_pandas()

    df["CharacterId"] = df["CharacterId"].astype(str)

    fig = px.bar(
        df,
        x="len",
        y="JOB",
        color="CharacterId",
        orientation="h",
        title="Job Leaderboard",
        labels={"len": "Count of Jobs", "JOB": "Job"},
        template="plotly_white",
    )

    fig.update_layout(
        title_font_size=20,
        title_x=0.5,
        xaxis_title_font_size=15,
        yaxis_title_font_size=15,
        showlegend=True,
        margin=dict(l=20, r=20, t=50, b=20),
        plot_bgcolor="white",
    )
    fig.update_xaxes(showgrid=True, gridwidth=0.5, gridcolor="lightgray")
    fig.update_yaxes(showgrid=True, gridwidth=0.5, gridcolor="lightgray")

    st.plotly_chart(fig)


def job_type_analytics(df: DataFrame, joined_data: DataFrame):
    df = df.with_columns(
        [
            pl.col("JobStarted").cast(pl.Utf8),
            pl.col("JobEnded").cast(pl.Utf8),
            pl.col("JobType").cast(pl.Utf8),
        ]
    )
    df = df.with_columns(
        [
            pl.col("JobStarted").str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S"),
            pl.col("JobEnded").str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S"),
        ]
    )

    # Total Jobs by Job Type
    total_jobs_by_type = df.groupby("JobType").count().rename({"count": "Job Count"})
    fig2 = px.bar(
        total_jobs_by_type.to_pandas(),
        x="JobType",
        y="Job Count",
        title="Total Jobs by Job Type",
    )

    st.plotly_chart(fig2)

    # Second Chart
    create_leaderboard_plot(joined_data)
