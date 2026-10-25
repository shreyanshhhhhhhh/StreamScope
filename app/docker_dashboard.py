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
BAR = "#4C8BF5"
PIE = px.colors.qualitative.Set2

FIND_GENRES = 'db.genre_interest.find({}, {_id: 0}).sort({rating_count: -1})'
AGG_PER_MOVIE = """db.genre_interest.aggregate([
  { $match: { movie_count: { $gt: 0 } } },
  { $project: {
      genre: 1, rating_count: 1, movie_count: 1, avg_rating: 1,
      ratings_per_movie: { $divide: ["$rating_count", "$movie_count"] }
  }},
  { $sort: { ratings_per_movie: -1 } }
])"""
FIND_TMDB = 'db.tmdb_providers.find({http_status: 200}, {genres: 1, "payload.results.IN.flatrate": 1})'


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


def _style(fig, height=420):
    fig.update_layout(
        template="plotly_dark",
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        height=height,
        margin=dict(l=8, r=8, t=28, b=8),
        font=dict(size=13),
        legend=dict(orientation="h", yanchor="top", y=-0.15),
    )
    return fig


def _bar(frame, x, y, color=BAR):
    fig = px.bar(frame, x=x, y=y, color_discrete_sequence=[color])
    fig.update_traces(marker_line_width=0)
    fig.update_xaxes(showgrid=False)
    fig.update_yaxes(gridcolor="rgba(255,255,255,0.08)")
    return _style(fig)


def _pie(frame, names, values):
    fig = px.pie(frame, names=names, values=values, color_discrete_sequence=PIE, hole=0.46)
    fig.update_traces(textposition="inside", textinfo="percent+label")
    return _style(fig, 460)


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
    documents = list(client[DATABASE][COLLECTION].find({}, {"_id": 0}).sort("rating_count", -1))
    client.close()
    if not documents:
        raise RuntimeError("streamscope.genre_interest is empty. Restart the container to upload the PySpark table.")
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
        frame["ratings_per_movie"] = (frame["rating_count"] / frame["movie_count"]).round(1)
    if "rater_count" in frame.columns:
        frame["ratings_per_rater"] = (frame["rating_count"] / frame["rater_count"]).round(2)
    frame["activity_rank"] = frame["rating_count"].rank(ascending=False, method="min").astype(int)
    if "avg_rating" in frame.columns:
        frame["rating_rank"] = frame["avg_rating"].rank(ascending=False, method="min").astype(int)
    if "ratings_per_movie" in frame.columns:
        frame["demand_rank"] = frame["ratings_per_movie"].rank(ascending=False, method="min").astype(int)
    return frame.sort_values("rating_count", ascending=False)


@st.cache_data(ttl=60, show_spinner=False)
def load_per_movie():
    client = _client()
    documents = list(
        client[DATABASE][COLLECTION].aggregate(
            [
                {"$match": {"movie_count": {"$gt": 0}}},
                {
                    "$project": {
                        "_id": 0,
                        "genre": 1,
                        "rating_count": 1,
                        "movie_count": 1,
                        "avg_rating": 1,
                        "ratings_per_movie": {"$round": [{"$divide": ["$rating_count", "$movie_count"]}, 1]},
                    }
                },
                {"$sort": {"ratings_per_movie": -1}},
            ]
        )
    )
    client.close()
    return pd.DataFrame(documents)


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


def _top_and_rest(frame, value, label):
    ordered = frame.sort_values(value, ascending=False)
    top = ordered.head(5)
    rest_value = float(ordered.iloc[5:][value].sum()) if len(ordered) > 5 else 0
    rows = top[["genre", value]].rename(columns={value: "value"})
    if rest_value:
        rows = pd.concat([rows, pd.DataFrame([{"genre": "All other genres", "value": rest_value}])], ignore_index=True)
    rows["group"] = label
    return rows


st.set_page_config(page_title="StreamScope", layout="wide", initial_sidebar_state="expanded")
st.markdown(
    """
    <style>
    .block-container { padding-top: 1.1rem; padding-bottom: 2rem; max-width: 1240px; }
    div[data-testid="stMetric"] {
        background: rgba(255,255,255,0.04);
        border: 1px solid rgba(255,255,255,0.08);
        border-radius: 12px;
        padding: 10px 14px;
    }
    div[data-testid="stTabs"] button { font-weight: 600; }
    </style>
    """,
    unsafe_allow_html=True,
)

st.title("StreamScope")
st.caption("Genre comparisons queried from MongoDB. The PySpark table is uploaded when the container starts. The 32,000,204 ratings stay on HDFS.")

if st.button("Upload and query MongoDB", type="primary"):
    with st.spinner("Uploading the genre table, then querying MongoDB"):
        upload_genres()
    load_genres.clear()
    load_per_movie.clear()
    load_coverage.clear()
    st.rerun()

try:
    genres = load_genres()
    per_movie = load_per_movie()
    coverage, providers, titles = load_coverage()
except Exception as exc:
    st.error("MongoDB query failed.")
    st.code(str(exc))
    st.stop()

with st.sidebar:
    st.header("Compare")
    picked = st.multiselect("Genres", genres["genre"].tolist(), default=genres["genre"].tolist())
    st.caption("A title in two genres is counted in both. Ratings run through 12 October 2023 and are not OTT views.")
    with st.expander("Queries this page runs"):
        st.code(FIND_GENRES, language="javascript")
        st.code(AGG_PER_MOVIE, language="javascript")
        st.code(FIND_TMDB, language="javascript")

view = genres[genres["genre"].isin(picked)].copy()
if view.empty:
    st.warning("Select at least one genre.")
    st.stop()
per_view = per_movie[per_movie["genre"].isin(picked)].copy() if not per_movie.empty else per_movie

one, two, three, four = st.columns(4)
one.metric("Genres stored", len(genres))
two.metric("Genres compared", len(view))
three.metric("Highest activity", view.sort_values("rating_count", ascending=False).iloc[0]["genre"])
four.metric("TMDB titles", titles)

tab_activity, tab_quality, tab_catalogue, tab_table, tab_mongo = st.tabs(
    ["Activity", "Quality", "Catalogue", "Comparison table", "MongoDB coverage"]
)

with tab_activity:
    left, right = st.columns(2)
    with left:
        st.subheader("Total ratings")
        st.plotly_chart(_bar(view, "genre", "rating_count"), width="stretch")
    with right:
        st.subheader("Share of all activity")
        st.plotly_chart(_pie(view, "genre", "rating_count"), width="stretch")
    st.subheader("Top 5 genres against the rest")
    st.plotly_chart(_pie(_top_and_rest(view, "rating_count", "activity"), "genre", "value"), width="stretch")

with tab_quality:
    if "avg_rating" not in view.columns:
        st.warning("The uploaded documents have no average rating.")
    else:
        left, right = st.columns(2)
        with left:
            st.subheader("Average rating")
            st.plotly_chart(
                _bar(view.sort_values("avg_rating", ascending=False), "genre", "avg_rating"),
                width="stretch",
            )
        with right:
            st.subheader("Activity against average rating")
            fig = px.scatter(
                view,
                x="rating_count",
                y="avg_rating",
                size="movie_count" if "movie_count" in view.columns else None,
                hover_name="genre",
                log_x=True,
                color_discrete_sequence=[BAR],
            )
            st.plotly_chart(_style(fig), width="stretch")
        ranked = view.sort_values("avg_rating", ascending=False)[
            ["genre", "avg_rating", "rating_count", "rating_rank", "activity_rank"]
        ]
        st.subheader("Ranked by average rating")
        st.dataframe(ranked, width="stretch", hide_index=True)

with tab_catalogue:
    if "movie_count" not in view.columns:
        st.info("Catalogue charts need movie_count in the uploaded table.")
    else:
        left, right = st.columns(2)
        with left:
            st.subheader("Share of the catalogue")
            st.plotly_chart(_pie(view, "genre", "movie_count"), width="stretch")
        with right:
            st.subheader("Ratings per movie")
            st.plotly_chart(
                _bar(view.sort_values("ratings_per_movie", ascending=False), "genre", "ratings_per_movie"),
                width="stretch",
            )
        left, right = st.columns(2)
        with left:
            st.subheader("Movies in each genre")
            st.plotly_chart(_bar(view.sort_values("movie_count", ascending=False), "genre", "movie_count"), width="stretch")
        with right:
            st.subheader("Distinct raters")
            st.plotly_chart(_bar(view.sort_values("rater_count", ascending=False), "genre", "rater_count"), width="stretch")
        if not per_view.empty:
            st.subheader("Aggregation result: ratings per movie")
            st.caption("This table is the MongoDB aggregation, not a second copy of the bar chart.")
            st.dataframe(per_view, width="stretch", hide_index=True)

with tab_table:
    columns = [
        column
        for column in (
            "activity_rank",
            "genre",
            "rating_count",
            "activity_share",
            "rater_count",
            "movie_count",
            "avg_rating",
            "rating_rank",
            "ratings_per_movie",
            "demand_rank",
            "ratings_per_rater",
        )
        if column in view.columns
    ]
    shown = view[columns].sort_values("activity_rank").copy()
    if "activity_share" in shown.columns:
        shown["activity_share"] = (shown["activity_share"] * 100).round(2)
    st.subheader("Side-by-side ranks")
    st.dataframe(shown, width="stretch", hide_index=True)

with tab_mongo:
    st.subheader("Subscription gap from tmdb_providers")
    if coverage.empty:
        st.warning(
            "No TMDB documents with HTTP 200 are in this MongoDB yet. "
            "The charts on the other tabs are the uploaded PySpark genre table. "
            "On the laptop, finish fetch_tmdb.py and click Upload and query MongoDB."
        )
    else:
        covered = coverage.copy()
        left, right = st.columns(2)
        with left:
            st.plotly_chart(_bar(covered, "genre", "gap"), width="stretch")
        with right:
            st.plotly_chart(_pie(covered, "genre", "titles"), width="stretch")
        if not providers.empty:
            st.subheader("Where the covered titles are available")
            st.plotly_chart(_pie(providers.head(8), "provider", "titles"), width="stretch")
        covered["coverage"] = (covered["coverage"] * 100).round(1)
        covered["gap"] = (covered["gap"] * 100).round(1)
        st.dataframe(covered, width="stretch", hide_index=True)

st.caption("Plotly draws the charts. MongoDB returns the rows. A multi-genre title counts once in each of its genres.")
