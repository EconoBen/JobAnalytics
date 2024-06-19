import polars as pl
import streamlit as st
from analytics import job_type_analytics
from topline import frequency


def read_data(path: str) -> pl.DataFrame:
    return pl.read_csv(path, try_parse_dates=False, has_header=True)


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
    col1.metric("Jobs Attempted", total_jobs)
    col2.metric("Average Jobs Per Week", int(round(avg_jobs_per_week, 0)))

    # Ensure completion and abandon counts are numeric before rounding
    completion_count = agg_df["Completion Count"].sum()
    abandon_count = agg_df["Abandon Count"].sum()

    ratio = int(round(completion_count / abandon_count, 0))

    col3.metric(
        "Completion/Abandon ratio",
        f"{ratio} : 1",
    )

    # Determine the most popular job type from freq_df

    job_popularity = freq_df.groupby("JobType").count().sort("count", descending=True)
    most_popular_job_type = job_popularity.select(pl.col("JobType"))[0, 0]
    least_popular_job = job_popularity.select(pl.col("JobType"))[-1, -1]

    # Additional metrics
    col4, col5, col6 = st.columns(3)
    col4.metric("Most Popular Job Type", most_popular_job_type)
    col5.metric("Least Popular Job Type", least_popular_job)

    ratio = int(round(job_popularity["count"][0] / job_popularity["count"][-1]))

    col6.metric(
        "Most/Least Popular Ratio",
        f"{ratio} : 1",
    )


def settings():
    st.set_page_config(layout="wide")
    st.sidebar.title("Chart Options")


def main():
    settings()
    st.sidebar.title("Wild Analytics Dashboard")
    page = st.sidebar.selectbox("Select Page", ["Top Line", "Job Type Analytics"])

    if page == "Top Line":
        st.title("Job Analytics")
        freq_df = read_data("data/WildRP Job Data - Raw Data.csv")
        agg_df = read_data("data/WildRP_job_data_values_only.csv")
        metrics(freq_df, agg_df)
        frequency(freq_df)
    elif page == "Job Type Analytics":
        st.title("Job Type Analytics")
        df = read_data("data/WildRP Job Data - Raw Data.csv")
        job_type_analytics(df)


if __name__ == "__main__":
    main()
