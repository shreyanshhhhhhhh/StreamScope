"""One-click HDFS fetch. PySpark reads the genre table; Plotly draws the charts."""

import os

import pandas as pd
import plotly.express as px
import streamlit as st

HDFS_GENRE = os.environ.get(
    "HDFS_GENRE_PATH",
    "hdfs://localhost:9000/streamscope/curated/genre_interest",
)

GENRES = [
    "Action", "Adventure", "Animation", "Children", "Comedy", "Crime",
    "Documentary", "Drama", "Fantasy", "Film-Noir", "Horror", "IMAX",
    "Musical", "Mystery", "Romance", "Sci-Fi", "Thriller", "War", "Western",
]

MEASURED_GENRES = [
    ("Drama", 13973271, 200914, 33152, 3.6825),
    ("Comedy", 11206925, 200693, 22447, 3.4324),
    ("Action", 9665213, 200208, 9296, 3.4764),
    ("Thriller", 8679464, 200168, 11555, 3.5317),
    ("Adventure", 7590522, 199968, 5156, 3.5234),
    ("Sci-Fi", 5717337, 197981, 4797, 3.4917),
    ("Romance", 5524615, 198689, 10048, 3.545),
    ("Crime", 5373051, 198878, 6704, 3.6918),
    ("Fantasy", 3702759, 193899, 3784, 3.5122),
    ("Children", 2731841, 182409, 4447, 3.4392),
    ("Mystery", 2615322, 188889, 3894, 3.6731),
    ("Horror", 2492315, 174469, 8468, 3.3072),
    ("Animation", 2214562, 173853, 4586, 3.6153),
    ("War", 1594110, 181626, 2225, 3.7917),
    ("IMAX", 1494179, 151832, 195, 3.5933),
    ("Musical", 1159516, 152251, 1033, 3.5543),
    ("Western", 596654, 134707, 1489, 3.6002),
    ("Documentary", 427353, 74356, 9103, 3.6912),
    ("Film-Noir", 304710, 83671, 350, 3.9158),
]

COLUMNS = ["genre", "rating_count", "rater_count", "movie_count", "avg_rating"]


def measured_frame():
    return pd.DataFrame(MEASURED_GENRES, columns=COLUMNS)


def read_hdfs(path):
    import sys
    import types

    try:
        import distutils  # noqa: F401
    except ModuleNotFoundError:
        distutils = types.ModuleType("distutils")
        version = types.ModuleType("distutils.version")

        class LooseVersion:
            def __init__(self, value):
                self.vstring = str(value)
                parts = []
                for piece in self.vstring.replace("-", ".").split("."):
                    digits = ""
                    for character in piece:
                        if character.isdigit():
                            digits += character
                        else:
                            break
                    parts.append(int(digits) if digits else 0)
                self.version = tuple(parts)

            def _other(self, other):
                if isinstance(other, LooseVersion):
                    return other.version
                return LooseVersion(other).version

            def __lt__(self, other):
                return self.version < self._other(other)

            def __le__(self, other):
                return self.version <= self._other(other)

            def __gt__(self, other):
                return self.version > self._other(other)

            def __ge__(self, other):
                return self.version >= self._other(other)

            def __eq__(self, other):
                return self.version == self._other(other)

        version.LooseVersion = LooseVersion
        distutils.version = version
        sys.modules["distutils"] = distutils
        sys.modules["distutils.version"] = version

    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder.appName("streamscope-dashboard")
        .master("local[*]")
        .config("spark.driver.memory", "1g")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    return spark.read.csv(path, header=True, inferSchema=True).toPandas()


@st.cache_data(show_spinner=False)
def load_genres():
    try:
        return read_hdfs(HDFS_GENRE), "pyspark", ""
    except Exception as exc:
        return measured_frame(), "saved-copy", str(exc)


def prepare(frame):
    frame = frame[frame["genre"].isin(GENRES)].copy()
    for column in ("rating_count", "rater_count", "movie_count"):
        frame[column] = frame[column].astype(int)
    frame["avg_rating"] = frame["avg_rating"].astype(float)
    total = int(frame["rating_count"].sum())
    frame["activity_share"] = frame["rating_count"] / total if total else 0
    return frame.sort_values("rating_count", ascending=False)


st.set_page_config(page_title="StreamScope", layout="wide")
st.title("StreamScope")
st.subheader("Fetch the Hadoop genre table and compare genres")
st.write("One click runs PySpark against " + HDFS_GENRE + " on this machine.")

if st.button("Fetch from Hadoop", type="primary"):
    load_genres.clear()
    with st.spinner("PySpark is reading HDFS"):
        frame, mode, error = load_genres()
    st.session_state["frame"] = prepare(frame)
    st.session_state["mode"] = mode
    st.session_state["error"] = error

if "frame" not in st.session_state:
    st.info("Click Fetch from Hadoop. The charts appear after PySpark returns the rows.")
    st.stop()

genres = st.session_state["frame"]
mode = st.session_state["mode"]
error = st.session_state["error"]

if mode == "pyspark":
    st.success("PySpark read this table from " + HDFS_GENRE)
else:
    st.error("PySpark did not read HDFS. Showing the saved Spark result instead.")
    st.code(error)

names = genres["genre"].tolist()
picked = st.sidebar.multiselect("Genres to compare", names, default=names[:8])
view = genres[genres["genre"].isin(picked)]
if view.empty:
    st.warning("Select at least one genre.")
    st.stop()

one, two, three = st.columns(3)
one.metric("Data path", "PySpark from HDFS" if mode == "pyspark" else "Saved copy")
two.metric("Genres compared", len(view))
three.metric("Highest activity", view.iloc[0]["genre"])

left, right = st.columns(2)
with left:
    st.subheader("Activity comparison")
    st.plotly_chart(px.bar(view, x="genre", y="rating_count"), width="stretch")
with right:
    st.subheader("Average-rating comparison")
    st.plotly_chart(
        px.bar(view.sort_values("avg_rating", ascending=False), x="genre", y="avg_rating"),
        width="stretch",
    )

st.subheader("Activity against average rating")
st.plotly_chart(
    px.scatter(
        view,
        x="rating_count",
        y="avg_rating",
        size="movie_count",
        hover_name="genre",
        labels={"rating_count": "Rating count", "avg_rating": "Average rating", "movie_count": "Movies"},
    ),
    width="stretch",
)
st.caption(
    "Point size is the number of movies. Drama can lead activity while Film-Noir leads the average. "
    "A movie with two genres counts in both. These are MovieLens ratings through 12 October 2023, not OTT views."
)

st.subheader("Comparison table")
shown = view[COLUMNS + ["activity_share"]].copy()
shown["activity_share"] = (shown["activity_share"] * 100).round(2)
st.dataframe(shown, width="stretch", hide_index=True)
