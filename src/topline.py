import plotly.express as px
import polars as pl
import streamlit as st

# color_map = {
#     "Deposit": "palecrimson",
#     "Delivery": "palegoldenrod",
#     "Herding": "mediumseagreen",
#     "Hunting": "lightskyblue",
#     "RanchChores": "lightsteelblue",
# }
# color_map = {
#     "Deposit": "crimson",
#     "Delivery": "goldenrod",
#     "Herding": "forestgreen",
#     "Hunting": "royalblue",
#     "RanchChores": "mediumpurple",
# }
color_map = {
    "Deposit": "crimson",
    "Delivery": "gold",
    "Herding": "forestgreen",
    "Hunting": "royalblue",
    "RanchChores": "mediumpurple",
}


def frequency(df: pl.DataFrame):
    st.header("Job Frequency")

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

    job_freq = (
        df.groupby(df["JobStarted"].dt.date())
        .count()
        .rename({"count": "Job Frequency"})
    )

    job_freq = (
        df.groupby([pl.col("JobStarted").dt.date(), "JobType"])
        .count()
        .rename({"count": "Job Frequency"})
    )

    # Sorting job types by total frequency
    total_counts = job_freq.groupby("JobType").agg(
        pl.sum("Job Frequency").alias("Total Frequency")
    )
    sorted_job_types = total_counts.sort("Total Frequency", descending=True)[
        "JobType"
    ].to_list()

    fig = px.bar(
        job_freq.to_pandas(),
        x="JobStarted",
        y="Job Frequency",
        color="JobType",
        color_discrete_map=color_map,
        category_orders={"JobType": sorted_job_types},
        labels={"Job Frequency": "Job Frequency", "JobStarted": "Job Started"},
    )
    fig.update_layout(
        autosize=False,
        width=5000,
        height=600,
        margin=dict(l=40, r=40, t=40, b=40),
        paper_bgcolor="white",
        plot_bgcolor="white",
        xaxis=dict(
            showline=True,
            showgrid=False,
            showticklabels=True,
            linecolor="black",
            linewidth=2,
        ),
        yaxis=dict(
            showline=True,
            showgrid=True,
            showticklabels=True,
            linecolor="black",
            linewidth=2,
        ),
        font=dict(family="Arial", size=12, color="black"),
    )

    # Display the chart
    st.plotly_chart(fig)
