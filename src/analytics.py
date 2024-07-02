import plotly.express as px
import polars as pl
import streamlit as st


def job_type_analytics(df: pl.DataFrame):
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
