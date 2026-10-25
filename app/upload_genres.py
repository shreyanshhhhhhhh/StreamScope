"""Upload the PySpark genre table into the local MongoDB.

The raw MovieLens files stay on HDFS. This loads the small curated table
(genre, rating count, rater count, movie count, average rating) into
streamscope.genre_interest so the dashboard can query it.
"""

import os
import time

import pandas as pd
from pymongo import MongoClient

GENRE_CSV = os.environ.get("GENRE_CSV", "/data/genre_interest.csv")
MONGO_URI = os.environ.get("MONGO_URI", "mongodb://mongo:27017")
DATABASE = "streamscope"
COLLECTION = "genre_interest"

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


def wait_for_client():
    last_error = None
    for _ in range(30):
        try:
            client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=2000)
            client.admin.command("ping")
            return client
        except Exception as exc:
            last_error = exc
            time.sleep(2)
    raise RuntimeError("MongoDB did not answer at " + MONGO_URI) from last_error


def read_genre_csv(path):
    frame = pd.read_csv(path)
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
    if frame.empty:
        raise ValueError("Genre CSV has none of the 19 MovieLens genres.")
    return frame


def upload_genres():
    frame = read_genre_csv(GENRE_CSV)
    client = wait_for_client()
    collection = client[DATABASE][COLLECTION]
    documents = []
    for row in frame.to_dict(orient="records"):
        document = {
            "genre": row["genre"],
            "rating_count": int(row["rating_count"]),
            "stored_from": "pyspark-hdfs-genre-table",
        }
        if "rater_count" in frame.columns and pd.notna(row.get("rater_count")):
            document["rater_count"] = int(row["rater_count"])
        if "movie_count" in frame.columns and pd.notna(row.get("movie_count")):
            document["movie_count"] = int(row["movie_count"])
        if "avg_rating" in frame.columns and pd.notna(row.get("avg_rating")):
            document["avg_rating"] = float(row["avg_rating"])
        documents.append(document)
    collection.delete_many({})
    collection.insert_many(documents)
    stored = collection.count_documents({})
    client.close()
    print("UPLOADED", stored, "genres into", DATABASE + "." + COLLECTION)
    return stored


if __name__ == "__main__":
    upload_genres()
