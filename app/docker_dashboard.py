"""Query MongoDB and draw the StreamScope comparisons.

upload_genres.py loads the PySpark genre table before this process starts.
TMDB watch-provider documents, when present, are queried from tmdb_providers.
"""

import os
import time

import pandas as pd
import plotly.express as px
import streamlit as st
from pymongo import MongoClient

from upload_genres import COLLECTION, DATABASE, GENRES, upload_genres

MONGO_URI = os.environ.get("MONGO_URI", "mongodb://mongo:27017")


def _client():
    last_error = None
    for _ in range(15):
        try:
            client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=2000)
            client.admin.command("ping")
            return client
        except Exception as exc:
            last_error = exc
            time.sleep(2)
    raise RuntimeError("MongoDB did not answer at " + MONGO_URI) from last_error


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
def load_genres():
    client = _client()
    documents = list(
        client[DATABASE][COLLECTION].find({}, {"_id": 0}).sort("rating_count", -1)
    )
    client.close()
    if not documents:
        raise RuntimeError(
            "streamscope.genre_interest is empty. Start the container so the PySpark table is uploaded."
        )
    frame = pd.DataFrame(documents)
    frame = frame[frame["genre"].isin(GENRES)].copy()
    frame["rating_count"] = frame["rating_count"].astype(int)
    for column in ("rater_count", "movie_count"):
        if column in frame.columns:
            frame[column] = frame[column].astype(int)
    if "avg_rating" in frame.columns:
        frame["avg_rating"] = frame["avg_rating"].astype(float)
    total = int(frame["rating_count"].sum())
    frame["activity_share"] = frame["rating_count"] / total if total else 0
    if "movie_count" in frame.columns:
        frame["ratings_per_movie"] = frame["rating_count"] / frame["movie_count"]
    if "rater_count" in frame.columns:
        frame["ratings_per_rater"] = frame["rating_count"] / frame["rater_count"]
    return frame.sort_values("rating_count", ascending=False)


@st.cache_data(ttl=60, show_spinner=False)
def load_coverage():
    client = _client()
    documents = client[DATABASE]["tmdb_providers"].find(
        {"http_status": {"$in": [200, "200"]}},
        {"genres": 1, "payload.results.IN.flatrate": 1, "payload.results.in.flatrate": 1},
    )
    genre_rows = []
    provider_rows = []
    titles = 0
    for doc in documents:
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
st.subheader("MongoDB queries over the PySpark genre table")
st.write(
    "The container uploads the HDFS genre table into "
    + DATABASE
    + "."
    + COLLECTION
    + ", then this page queries "
    + MONGO_URI
    + ". The 32,000,204 ratings stay on HDFS."
)

if st.button("Upload and query MongoDB", type="primary"):
    with st.spinner("Uploading the genre table, then querying MongoDB"):
        upload_genres()
    load_genres.clear()
    load_coverage.clear()
    st.rerun()

try:
    genres = load_genres()
    coverage, providers, titles = load_coverage()
except Exception as exc:
    st.error("MongoDB query failed.")
    st.code(str(exc))
    st.stop()

picked = st.sidebar.multiselect(
    "Genres to compare",
    genres["genre"].tolist(),
    default=genres["genre"].tolist()[:8],
)
view = genres[genres["genre"].isin(picked)]
if view.empty:
    st.warning("Select at least one genre.")
    st.stop()

one, two, three = st.columns(3)
one.metric("Genres stored in MongoDB", len(genres))
two.metric("Genres compared", len(view))
three.metric("TMDB titles queried", titles)

tab_activity, tab_quality, tab_demand, tab_mongo = st.tabs(
    ["Activity", "Quality", "Demand per title", "MongoDB coverage"]
)

with tab_activity:
    left, right = st.columns(2)
    with left:
        st.subheader("Total ratings")
        st.plotly_chart(px.bar(view, x="genre", y="rating_count"), width="stretch")
    with right:
        st.subheader("Share of all activity")
        st.plotly_chart(px.pie(view, names="genre", values="rating_count", hole=0.45), width="stretch")
    st.caption(
        "Queried from streamscope.genre_interest. A title in two genres counts in both. "
        "MovieLens ratings through 12 October 2023, not OTT views."
    )

with tab_quality:
    if "avg_rating" not in view.columns:
        st.warning("The uploaded documents have no avg_rating field.")
    else:
        left, right = st.columns(2)
        with left:
            st.subheader("Average rating")
            st.plotly_chart(
                px.bar(
                    view.sort_values("avg_rating", ascending=False),
                    x="genre",
                    y="avg_rating",
                    range_y=[3.0, 4.0],
                ),
                width="stretch",
            )
        with right:
            st.subheader("Activity against average rating")
            st.plotly_chart(
                px.scatter(
                    view,
                    x="rating_count",
                    y="avg_rating",
                    size="movie_count" if "movie_count" in view.columns else None,
                    hover_name="genre",
                    log_x=True,
                ),
                width="stretch",
            )

with tab_demand:
    if "ratings_per_movie" not in view.columns:
        st.info("Ratings per movie need movie_count in the uploaded table.")
    else:
        left, right = st.columns(2)
        with left:
            st.subheader("Ratings per movie")
            st.caption("A high bar means a few titles carry a lot of interest.")
            st.plotly_chart(
                px.bar(
                    view.sort_values("ratings_per_movie", ascending=False),
                    x="genre",
                    y="ratings_per_movie",
                ),
                width="stretch",
            )
        with right:
            st.subheader("Movies in the genre")
            st.plotly_chart(
                px.bar(view.sort_values("movie_count", ascending=False), x="genre", y="movie_count"),
                width="stretch",
            )
        if "rater_count" in view.columns:
            st.subheader("Distinct raters")
            st.plotly_chart(
                px.bar(view.sort_values("rater_count", ascending=False), x="genre", y="rater_count"),
                width="stretch",
            )

with tab_mongo:
    st.subheader("Subscription gap queried from tmdb_providers")
    if coverage.empty:
        st.warning(
            "No TMDB documents with HTTP 200 are in this MongoDB yet. "
            "The genre charts above are the uploaded PySpark table. "
            "On the laptop, finish fetch_tmdb.py and click Upload and query MongoDB."
        )
    else:
        left, right = st.columns(2)
        with left:
            st.plotly_chart(px.bar(coverage, x="genre", y="gap"), width="stretch")
        with right:
            st.plotly_chart(px.bar(providers.head(12), x="provider", y="titles"), width="stretch")
        shown = coverage.copy()
        shown["coverage"] = (shown["coverage"] * 100).round(1)
        shown["gap"] = (shown["gap"] * 100).round(1)
        st.dataframe(shown, width="stretch", hide_index=True)

st.subheader("Rows returned by the genre query")
table_columns = [
    column
    for column in (
        "genre",
        "rating_count",
        "rater_count",
        "movie_count",
        "avg_rating",
        "ratings_per_movie",
        "activity_share",
    )
    if column in view.columns
]
shown = view[table_columns].copy()
if "activity_share" in shown.columns:
    shown["activity_share"] = (shown["activity_share"] * 100).round(2)
if "ratings_per_movie" in shown.columns:
    shown["ratings_per_movie"] = shown["ratings_per_movie"].round(0)
st.dataframe(shown, width="stretch", hide_index=True)
