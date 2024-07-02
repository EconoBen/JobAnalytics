import polars as pl


def task_analytics():
    jobs = pl.read_csv("data/unique_jobs.csv")
