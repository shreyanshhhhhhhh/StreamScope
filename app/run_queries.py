"""Run the MongoDB queries after the genre table has been uploaded.

Prints the result and stores one summary document in streamscope.query_runs.
"""

from datetime import datetime, timezone

from upload_genres import DATABASE, MONGO_URI, wait_for_client

FIND = {"_id": 0}
AGGREGATE = [
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


def main():
    client = wait_for_client()
    database = client[DATABASE]
    genres = list(database["genre_interest"].find({}, FIND).sort("rating_count", -1))
    per_movie = list(database["genre_interest"].aggregate(AGGREGATE))
    tmdb_ok = database["tmdb_providers"].count_documents({"http_status": {"$in": [200, "200"]}})
    if not genres or not per_movie:
        raise RuntimeError("genre_interest has no documents to query")

    top_activity = genres[0]
    top_demand = per_movie[0]
    print("QUERY genre_interest.find count", len(genres))
    print(
        "HIGHEST_ACTIVITY",
        top_activity["genre"],
        top_activity["rating_count"],
        "avg",
        top_activity.get("avg_rating"),
    )
    print(
        "HIGHEST_RATINGS_PER_MOVIE",
        top_demand["genre"],
        top_demand["ratings_per_movie"],
        "movies",
        top_demand["movie_count"],
    )
    print("QUERY tmdb_providers http 200 count", tmdb_ok)
    for row in per_movie[:5]:
        print(
            "ROW",
            row["genre"],
            "ratings_per_movie",
            row["ratings_per_movie"],
            "rating_count",
            row["rating_count"],
        )

    database["query_runs"].insert_one(
        {
            "ran_at": datetime.now(timezone.utc).isoformat(),
            "mongo_uri": MONGO_URI,
            "genre_documents": len(genres),
            "highest_activity": top_activity["genre"],
            "highest_activity_ratings": top_activity["rating_count"],
            "highest_ratings_per_movie": top_demand["genre"],
            "ratings_per_movie": top_demand["ratings_per_movie"],
            "tmdb_http_200": tmdb_ok,
        }
    )
    client.close()


if __name__ == "__main__":
    main()
