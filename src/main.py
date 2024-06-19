import plotly.express as px
import polars as pl
import streamlit as st


def read_data(path: str) -> pl.DataFrame:
    return pl.read_csv(path, try_parse_dates=False, has_header=True)


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

    fig = px.bar(
        job_freq.to_pandas(),
        x="JobStarted",
        y="Job Frequency",
        color="JobType",
        labels={"Job Frequency": "Job Frequency", "JobStarted": "Job Started"},
    )
    fig.update_layout(
        autosize=False,
        width=1000,
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


def metrics(freq_df: pl.DataFrame, agg_df: pl.DataFrame):
    freq_df = freq_df.with_columns(
        [
            pl.col("JobStarted").str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S"),
            pl.col("JobEnded").str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S"),
        ]
    )
    freq_df = freq_df.with_columns(
        [
            pl.col("JobStarted").dt.month().alias("Month"),
            pl.col("JobStarted").dt.week().alias("Week"),
            pl.col("JobStarted").dt.day().alias("Day"),
        ]
    )

    avg_jobs_per_week = (
        freq_df.groupby("Week").count().select(pl.col("count").mean())[0, 0]
    )
    total_jobs = freq_df.shape[0]

    # Display metrics
    col1, col2, col3 = st.columns(3)
    col1.metric("Total Number of Jobs", total_jobs)
    col2.metric("Average Jobs Per Week", round(avg_jobs_per_week, 2))

    # Ensure completion and abandon counts are numeric before rounding
    completion_count = agg_df["Completion Count"].sum()
    abandon_count = agg_df["Abandon Count"].sum()

    ratio = int(round(completion_count / abandon_count, 0))

    col3.metric(
        "Completion/Abandon ratio",
        f"{ratio} : 1",
    )


def settings():
    st.set_page_config(layout="wide")
    st.sidebar.title("Chart Options")


def main():
    settings()
    st.title("Wild Analytics Dashboard")
    chart_type = st.sidebar.selectbox("Select Chart Type", ["Top Line"])

    if chart_type == "Top Line":
        freq_df: pl.DataFrame = read_data("data/WildRP Job Data - Raw Data.csv")
        agg_df: pl.DataFrame = read_data("data/WildRP_job_data_values_only.csv")
        metrics(freq_df, agg_df)
        frequency(freq_df)


if __name__ == "__main__":
    main()
