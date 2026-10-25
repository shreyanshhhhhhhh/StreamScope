# StreamScope

Single-node Hadoop pipeline for MovieLens genre demand, with a Dockerized Streamlit app that queries TMDB watch-provider documents stored in MongoDB.

Hadoop and PySpark stay on the host. When the dashboard container starts, it uploads the curated PySpark genre table into MongoDB collection `streamscope.genre_interest`, then the page queries that collection. TMDB watch-provider JSON, once fetched, is queried from `streamscope.tmdb_providers`. The 32 million ratings are not copied into MongoDB.

## Run the Docker app

From WSL, after Hadoop is up and `streamscope-mongo` has TMDB documents:

```bash
sudo service docker start
hdfs dfs -getmerge /streamscope/curated/genre_interest ~/streamscope/output/genre_interest.csv
cd ~/streamscope/app
docker compose up -d --build
docker ps
```

Open http://localhost:8502 and click **Query MongoDB**.

`docker compose` is run from `~/streamscope/app` so the existing Mongo volume `app_mongodata` is reused. Do not put `TMDB_API_KEY` in this directory or in the image.

The host dashboard that reads HDFS directly is `dashboard.py` at the repository root. Run that one outside Docker, from the virtualenv, on port 8501.
