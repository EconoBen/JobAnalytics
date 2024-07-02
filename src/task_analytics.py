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


def create_plot(data: DataFrame):
    fig = px.scatter(
        data.to_pandas(),
        x="JobTemplateId",
        y="JOB",
        title="Task Analytics",
        labels={"JobTemplateId": "Job Template ID", "JOB": "Job"},
        template="plotly_white",
    )
    fig.update_layout(
        title_font_size=20,
        title_x=0.5,
        xaxis_title_font_size=15,
        yaxis_title_font_size=15,
        showlegend=False,
        margin=dict(l=20, r=20, t=50, b=20),
        plot_bgcolor="white",
    )
    fig.update_xaxes(showgrid=True, gridwidth=0.5, gridcolor="lightgray")
    fig.update_yaxes(showgrid=True, gridwidth=0.5, gridcolor="lightgray")

    return fig


def task_analytics():
    st.title("Task Analytics Dashboard")

    data = load_and_process_data()

    fig = create_plot(data)

    st.plotly_chart(fig)
