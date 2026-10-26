# StreamScope — storage, pipeline, gaps, commands, Docker (viva guide)

Simple reference for presentation and viva. Everything below matches what this repo implements.

---

## 1. Full pipeline (say this order in the demo)

```text
[1] Download MovieLens 32M on laptop
         |
         v
[2] HDFS  /streamscope/raw/movielens/ratings.csv, movies.csv, links.csv
         |     (NameNode = index, DataNode = block files on disk)
         v
[3] PySpark on WSL host (NOT inside Docker)
         |     count ratings -> 32,000,204
         |     aggregate genres -> /streamscope/curated/genre_interest
         |     top 2000 titles   -> /streamscope/curated/catalog_universe
         v
[4] Small file out of HDFS
         |     hdfs dfs -getmerge -> ~/streamscope/output/genre_interest.csv
         v
[5] Docker Compose (~/streamscope/app)
         |     streamscope-mongo     (database streamscope)
         |     streamscope-dashboard (upload CSV -> query Mongo -> Streamlit :8502)
         v
[6] Optional on host (same Mongo port 27017)
         fetch_tmdb.py -> streamscope.tmdb_providers (JSON from TMDB, region IN)
         v
[7] Dashboard "MongoDB coverage" tab
         gap per genre from tmdb_providers (only when fetch is complete)
```

**Host dashboard (optional, not in Docker):** `dashboard.py` on port **8501** can read HDFS live with PySpark if the venv can import Spark.

---

## 2. Where data is stored (what lives where)

| What | Where physically | Path / name | Size (your run) |
|------|------------------|-------------|-----------------|
| Raw MovieLens zip & CSV | Laptop disk (ignored by git) | `~/streamscope/data/local/ml-32m/` | ratings ~837 MB local |
| HDFS blocks | DataNode dir | `/home/pandey/hadoopdata/datanode` | DFS used ~849 MB |
| HDFS paths | Logical | `/streamscope/raw/movielens/*.csv` | ratings 7 blocks, 877,076,222 bytes |
| NameNode metadata | NameNode dir | `/home/pandey/hadoopdata/namenode` | small (index only) |
| Spark output (genre) | HDFS | `/streamscope/curated/genre_interest/` | one part CSV ~763 B |
| Spark output (catalog) | HDFS | `/streamscope/curated/catalog_universe/` | 2000 titles + header |
| Merged genre CSV | Laptop | `~/streamscope/output/genre_interest.csv` | 21 lines (19 genres + junk row + header) |
| MongoDB genre docs | Docker volume | volume `app_mongodata` → `/data/db` in mongo container | **19** documents in `genre_interest` |
| TMDB JSON | Same Mongo volume | `streamscope.tmdb_providers` | **Incomplete** until fetch finishes (~2000 target) |
| Query log | Mongo | `streamscope.query_runs` | one doc per container start |
| GitHub repo | Remote | `github.com/shreyanshhhhhhhh/StreamScope` | code only, no CSV/zip |

**Not used in this project:** cloud MongoDB (Atlas), HDFS inside Docker, Mahout, Pig scripts in the repo.

---

## 3. Content gap — two levels in StreamScope

### A) Catalogue gap from MovieLens only (implemented now)

This does **not** need TMDB. It is **ratings per movie** by genre:

```text
ratings_per_movie = rating_count / movie_count
```

**Example from your Spark table:**

- **IMAX:** ~7,662 ratings per movie (195 movies) → interest concentrated on few titles.
- **Documentary:** ~47 ratings per movie (9,103 movies) → large catalogue, low intensity per title.
- **Drama:** most **total** ratings, but ~422 per movie because there are 33,152 movies.

**Viva line:** “Gap here means **demand is high relative to how many titles carry it**, not yet OTT availability.”

### B) OTT subscription gap (designed, needs full TMDB fetch)

For each genre, over the **2,000** studied titles (multi-genre titles count in each genre):

```text
coverage(genre) = titles_with_India_flatrate_provider / titles_stored_in_Mongo_for_that_genre
gap(genre)      = 1 - coverage
```

- **Provider:** TMDB `watch/providers`, region **IN**, list **flatrate** (subscription).
- **Stored in:** `tmdb_providers.payload.results.IN.flatrate`.
- **Dashboard:** “MongoDB coverage” tab; empty if `http_status: 200` count is 0.

**Honest gap in project:** fetch was started but **not proven complete at 2000** documents, so do not quote OTT gap percentages until `db.tmdb_providers.countDocuments({http_status:200})` is near 2000.

---

## 4. MongoDB — if and how it is used

**Yes, MongoDB is required for the Docker rubric row** (store + query unstructured/semi-structured data).

| Collection | Source | Purpose |
|------------|--------|---------|
| `genre_interest` | PySpark CSV uploaded at container start | `find` + `aggregate` for charts and tables |
| `tmdb_providers` | `fetch_tmdb.py` on host | Nested JSON; query for India flatrate providers |
| `query_runs` | `run_queries.py` at container start | Proof that queries ran (Drama, IMAX, etc.) |

**Connection from dashboard container:** `mongodb://mongo:27017` (service name `mongo` in compose).

**Not in MongoDB:** the 32M ratings file (stays on HDFS only).

---

## 5. “SQL” in this project (Spark SQL + Mongo, not MySQL)

You did **not** use a separate RDBMS. Examiners often say “SQL” meaning **queries**:

### PySpark (on host) — same idea as SQL

Conceptually:

```sql
-- Conceptual only; your job is in PySpark Python API
SELECT genre,
       COUNT(*) AS rating_count,
       COUNT(DISTINCT userId) AS rater_count,
       COUNT(DISTINCT movieId) AS movie_count,
       ROUND(AVG(rating), 4) AS avg_rating
FROM ratings JOIN movies ON movieId
LATERAL VIEW explode(split(genres, '|')) g AS genre
WHERE genre NOT IN ('', '(no genres listed)')
GROUP BY genre;
```

**Count job output:** `RATINGS_ON_HDFS 32000204`.

### MongoDB shell (used by dashboard + run_queries.py)

```javascript
use streamscope

db.genre_interest.find({}, {_id: 0}).sort({rating_count: -1})

db.genre_interest.aggregate([
  { $match: { movie_count: { $gt: 0 } } },
  { $project: {
      genre: 1,
      rating_count: 1,
      movie_count: 1,
      avg_rating: 1,
      ratings_per_movie: { $divide: ["$rating_count", "$movie_count"] }
  }},
  { $sort: { ratings_per_movie: -1 } }
])

db.tmdb_providers.countDocuments({ http_status: 200 })

db.tmdb_providers.findOne(
  { http_status: 200 },
  { title: 1, genres: 1, "payload.results.IN.flatrate.provider_name": 1 }
)

db.query_runs.find().sort({ _id: -1 }).limit(1)
```

---

## 6. Important commands

### SSH (needed before start-dfs.sh)

```bash
sudo service ssh start
ssh localhost    # should login without password if keys are set up
```

Hadoop start scripts use SSH to launch NameNode/DataNode on `localhost`.

### Hadoop environment (each new terminal)

```bash
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export HADOOP_HOME=$HOME/hadoop
export PATH=$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH
```

### Hadoop — start and check

```bash
start-dfs.sh
jps
# Expect: NameNode, DataNode, SecondaryNameNode

hdfs dfsadmin -safemode get    # OFF
hdfs dfsadmin -report          # Live datanodes (1)

hdfs dfs -ls /streamscope/raw/movielens
hdfs fsck /streamscope/raw/movielens/ratings.csv -files -blocks -locations
```

### Hadoop — upload and export (already done in your project)

```bash
hdfs dfs -mkdir -p /streamscope/raw/movielens
hdfs dfs -put ~/streamscope/data/local/ml-32m/ratings.csv /streamscope/raw/movielens/
hdfs dfs -put ~/streamscope/data/local/ml-32m/movies.csv /streamscope/raw/movielens/
hdfs dfs -put ~/streamscope/data/local/ml-32m/links.csv /streamscope/raw/movielens/

hdfs dfs -getmerge /streamscope/curated/genre_interest ~/streamscope/output/genre_interest.csv
```

**NameNode UI:** http://localhost:9870

**If start-dfs.sh says “already running”:** cluster is up; do not format NameNode again.

### PySpark (on host)

```bash
export SPARK_HOME=$HOME/spark-3.5.9-bin-hadoop3
export HADOOP_CONF_DIR=$HOME/hadoop/etc/hadoop
export PATH=$SPARK_HOME/bin:$PATH

spark-submit --version
spark-submit jobs/count_ratings.py
spark-submit jobs/aggregate_genres.py
spark-submit jobs/catalog_universe.py
```

### TMDB fetch (host, needs API key in env)

```bash
cd ~/streamscope
source venv/bin/activate
export TMDB_API_KEY='your_short_v3_key'
python jobs/fetch_tmdb.py
```

### Docker + Mongo

```bash
sudo service docker start
cd ~/streamscope/app
sudo docker compose up -d --build
sudo docker ps

sudo docker logs streamscope-dashboard | tail -20
# Expect: UPLOADED 19 genres ... HIGHEST_ACTIVITY Drama ... HIGHEST_RATINGS_PER_MOVIE IMAX

sudo docker exec streamscope-mongo mongosh --quiet streamscope --eval \
  'print("genres", db.genre_interest.countDocuments({})); print("tmdb_ok", db.tmdb_providers.countDocuments({http_status:200}))'
```

**App URL:** http://localhost:8502

---

## 7. Dockerfile and related files (line by line)

**File:** `app/Dockerfile`

| Line | Meaning |
|------|---------|
| `FROM python:3.11-slim` | Base image: Python 3.11, small Debian. No Java/Hadoop/Spark inside. |
| `WORKDIR /app` | All app files live in `/app` in the container. |
| `COPY requirements.txt` + `RUN pip install ...` | Installs streamlit, plotly, pandas, pymongo, requests at **build** time. |
| `COPY docker_dashboard.py upload_genres.py run_queries.py entrypoint.sh .` | Application code copied into image. |
| `RUN chmod +x entrypoint.sh` | Startup script must be executable. |
| `EXPOSE 8502` | Documents the Streamlit port (compose maps host 8502 → container 8502). |
| `HEALTHCHECK ... _stcore/health` | Docker marks container **healthy** when Streamlit responds. |
| `CMD ["./entrypoint.sh"]` | On start: upload genres → run queries → run Streamlit. |

**File:** `app/entrypoint.sh`

1. `python upload_genres.py` — reads `/data/genre_interest.csv` (mounted from `../output`), writes **19** docs to `genre_interest`.
2. `python run_queries.py` — runs `find` + `aggregate`, prints Drama / IMAX, inserts into `query_runs`.
3. `streamlit run docker_dashboard.py ...` — UI on `0.0.0.0:8502`.

**File:** `app/docker-compose.yml`

| Service | Role |
|---------|------|
| `mongo` | Official `mongo:7`, port 27017, persistent volume `mongodata`. |
| `dashboard` | Builds from `Dockerfile`, depends on `mongo`, mounts `../output` read-only at `/data`. |

**Environment in dashboard:**

- `MONGO_URI=mongodb://mongo:27017`
- `GENRE_CSV=/data/genre_interest.csv`

**What is NOT in the image:** MovieLens zip, HDFS, Spark, TMDB API key, raw ratings.

---

## 8. Gaps in the project (be honest in viva)

| Area | Status | What to say |
|------|--------|-------------|
| HDFS upload + fsck + 1 DataNode | Done | Show fsck HEALTHY, 7 blocks, count 32M |
| PySpark genre aggregate | Done | 19 genres, Drama top activity |
| Docker + Mongo genre query | Done | 19 docs, run_queries output, :8502 |
| TMDB → Mongo full 2000 docs | **Incomplete / unproven** | Coverage tab empty or warning |
| OTT gap chart | **Blocked** until TMDB complete | Formula ready in code |
| Host Streamlit live HDFS (8501) | **May fail** on Python 3.14 / distutils | Use Docker demo for Mongo; terminal Spark for HDFS proof |
| Mahout / Pig | **Not in repo** | Spark used instead of Pig; no Mahout ML model |
| Multi-node Hadoop | **Not in scope** | replication=1, one machine |
| Report plagiarism / AI | **Your responsibility** | Rewrite report in own words |
| Presentation / recorded backup | **Your task** | Follow pipeline order in demo |

---

## 9. Mahout and Pig (if examiner asks)

- **Pig Latin:** High-level scripts on Hadoop for ETL (`LOAD`, `GROUP`). Your **PySpark** jobs do the same: read HDFS CSV, join, explode genres, aggregate, write curated path.
- **Mahout:** ML on Hadoop (e.g. recommendations). **Not used.** You did **aggregates**, not collaborative filtering. Optional future: recommend movies on HDFS, then compare genres to TMDB coverage.

---

## 10. One-minute viva closing

“Data lives on **HDFS** in blocks; **Spark** proved **32,000,204** ratings and built the **genre table**. A **small CSV** is merged to the laptop and **uploaded to MongoDB** inside **Docker**. **Streamlit** queries Mongo and draws comparisons; **ratings per movie** is our MovieLens-side gap signal. **TMDB JSON** in the same Mongo will give **subscription gap** once the fetch to **2000** titles is complete. **Hadoop is outside Docker**; the container only mounts the curated file and talks to **mongo:27017**.”
