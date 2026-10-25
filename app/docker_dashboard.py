"""Streamlit app for the Docker mark.

The genre table is the PySpark aggregate already stored on HDFS, copied out
with `hdfs dfs -getmerge` and mounted into the container. Watch-provider JSON
is queried from MongoDB. Hadoop stays on the host: inside a container,
localhost:9000 is not the NameNode.
"""

import os
import time

import pandas as pd
import plotly.express as px
import streamlit as st
from pymongo import MongoClient

GENRE_CSV = os.environ.get("GENRE_CSV", "/data/genre_interest.csv")
MONGO_URI = os.environ.get("MONGO_URI", "mongodb://mongo:27017")

GENRES = [
    "Action", "Adventure", "Animation", "Children", "Comedy", "Crime",
    "Documentary", "Drama", "Fantasy", "Film-Noir", "Horror", "IMAX",
    "Musical", "Mystery", "Romance", "Sci-Fi", "Thriller", "War", "Western",
]

COLUMN_ALIASES = {
    "count": "rating_count",
    "count(rating)": "rating_count",
    "count(userId)": "rater_count",
    "count(DISTINCT userId)": "rater_count",
    "count(movieId)": "movie_count",
    "count(DISTINCT movieId)": "movie_count",
    "avg(rating)": "avg_rating",
    "round(avg(rating), 4)": "avg_rating",
}


@st.cache_data(show_spinner=False)
def load_genres():
    frame = pd.read_csv(GENRE_CSV)
    frame = frame.rename(columns={name: COLUMN_ALIASES.get(name, name) for name in frame.columns})
    missing = [name for name in ("genre", "rating_count") if name not in frame.columns]
    if missing:
        raise ValueError(
            "Genre CSV is missing "
            + ", ".join(missing)
            + ". Columns found: "
            + ", ".join(map(str, frame.columns))
        )
    frame = frame[frame["genre"].isin(GENRES)].copy()
    frame["rating_count"] = frame["rating_count"].astype(int)
    return frame.sort_values("rating_count", ascending=False)


def _genre_names(raw):
    if isinstance(raw, list):
        return [str(name) for name in raw]
    return [name for name in str(raw or "").split("|") if name]


def _flatrate(doc):
    payload = doc.get("payload") or {}
    results = payload.get("results") or {}
    india = results.get("IN") or results.get("in") or {}
    flatrate = india.get("flatrate") or []
    return flatrate if isinstance(flatrate, list) else []


@st.cache_data(ttl=60, show_spinner=False)
def load_coverage():
    last_error = None
    client = None
    for _ in range(15):
        try:
            client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=2000)
            client.admin.command("ping")
            break
        except Exception as exc:
            last_error = exc
            client = None
            time.sleep(2)
    if client is None:
        raise RuntimeError("MongoDB did not answer at " + MONGO_URI) from last_error

    docs = client["streamscope"]["tmdb_providers"].find(
        {"http_status": {"$in": [200, "200"]}},
        {"genres": 1, "payload.results.IN.flatrate": 1, "payload.results.in.flatrate": 1},
    )
    genre_rows = []
    provider_rows = []
    titles = 0
    for doc in docs:
        titles += 1
        providers = _flatrate(doc)
        covered = 1 if providers else 0
        for genre in _genre_names(doc.get("genres")):
            if genre in GENRES:
                genre_rows.append({"genre": genre, "covered": covered})
        for provider in providers:
            name = provider.get("provider_name") if isinstance(provider, dict) else None
            if name:
                provider_rows.append({"provider": name})
    client.close()

    if genre_rows:
        coverage = pd.DataFrame(genre_rows).groupby("genre", as_index=False).agg(
            titles=("covered", "size"),
            on_ott=("covered", "sum"),
        )
        coverage["coverage"] = coverage["on_ott"] / coverage["titles"]
        coverage["gap"] = 1 - coverage["coverage"]
        coverage = coverage.sort_values("gap", ascending=False)
    else:
        coverage = pd.DataFrame(columns=["genre", "titles", "on_ott", "coverage", "gap"])

    if provider_rows:
        providers = (
            pd.DataFrame(provider_rows)
            .groupby("provider", as_index=False)
            .size()
            .rename(columns={"size": "titles"})
            .sort_values("titles", ascending=False)
        )
    else:
        providers = pd.DataFrame(columns=["provider", "titles"])
    return coverage, providers, titles


st.set_page_config(page_title="StreamScope", layout="wide")
st.title("StreamScope")
st.subheader("Genre demand from PySpark, coverage from MongoDB")
st.write(
    "This container reads the PySpark genre table from "
    + GENRE_CSV
    + " and queries unstructured TMDB documents in MongoDB at "
    + MONGO_URI
    + "."
)

if st.button("Query MongoDB", type="primary"):
    load_coverage.clear()
    st.rerun()

try:
    genres = load_genres()
    coverage, providers, titles = load_coverage()
except Exception as exc:
    st.error("Could not read the PySpark CSV or MongoDB.")
    st.code(str(exc))
    st.stop()

one, two, three = st.columns(3)
one.metric("Genres from the HDFS table", len(genres))
two.metric("TMDB titles queried", titles)
three.metric("Highest gap", coverage.iloc[0]["genre"] if not coverage.empty else "—")

st.subheader("Demand computed by PySpark")
st.plotly_chart(px.bar(genres, x="genre", y="rating_count"), width="stretch")
st.caption(
    "This file is the PySpark aggregate stored on HDFS. A title in two genres counts in both. "
    "MovieLens ratings through 12 October 2023, not OTT views."
)

st.subheader("Subscription gap queried from MongoDB")
if coverage.empty:
    st.warning("No TMDB documents with HTTP 200 yet. Let fetch_tmdb.py finish, then click Query MongoDB.")
else:
    left, right = st.columns(2)
    with left:
        st.plotly_chart(
            px.bar(coverage, x="genre", y="gap", labels={"gap": "Gap (1 − coverage)"}),
            width="stretch",
        )
    with right:
        st.plotly_chart(
            px.bar(providers.head(12), x="provider", y="titles"),
            width="stretch",
        )
    st.caption(
        "Gap = 1 − (titles with an India flatrate provider / titles stored). "
        "Region IN, subscription providers only. A multi-genre title counts in each genre."
    )
    shown = coverage.copy()
    shown["coverage"] = (shown["coverage"] * 100).round(1)
    shown["gap"] = (shown["gap"] * 100).round(1)
    st.dataframe(shown, width="stretch", hide_index=True)
