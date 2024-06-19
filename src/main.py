import plotly.express as px
import polars as pl
import streamlit as st


# Sample data creation (replace with your actual data loading)
def read_data():
    data: pl.DataFrame = pl.read_csv(
        "data/WildRP Job Data - Raw Data.csv", try_parse_dates=False, has_header=True
    )

    return data.with_columns(
        [pl.col("JobStarted").cast(pl.Utf8), pl.col("JobEnded").cast(pl.Utf8)]
    )


# Load data
df = read_data()

# Streamlit app
st.title("Job Frequency Analysis")
st.sidebar.title("Chart Options")

# Sidebar options
chart_type = st.sidebar.selectbox("Select Chart Type", ["Frequency"])

if chart_type == "Frequency":
    st.header("Job Frequency")

    # Convert JobStarted and JobEnded to datetime with time
    df = df.with_columns(
        [
            pl.col("JobStarted").str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S"),
            pl.col("JobEnded").str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S"),
        ]
    )

    # Calculate job frequency
    job_freq = (
        df.groupby(df["JobStarted"].dt.date())
        .count()
        .rename({"count": "Job Frequency"})
    )

    # Plotting with Plotly
    fig = px.bar(
        job_freq.to_pandas(),
        x="JobStarted",
        y="Job Frequency",
        labels={"Job Frequency": "Job Frequency"},
    )

    # Display the chart
    st.plotly_chart(fig)

    # Calculate job frequency
    job_freq = (
        df.groupby([pl.col("JobStarted").dt.date(), "JobType"])
        .count()
        .rename({"count": "Job Frequency"})
    )

    # Plotting with Plotly
    fig = px.bar(
        job_freq.to_pandas(),
        x="JobStarted",
        y="Job Frequency",
        color="JobType",
        labels={"Job Frequency": "Job Frequency", "JobStarted": "Job Started"},
    )

    # Display the chart
    st.plotly_chart(fig)
