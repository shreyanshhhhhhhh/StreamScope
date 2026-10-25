# StreamScope: Finding Content Gaps in a Large Ratings Collection with Hadoop, Spark, MongoDB, and Docker

**Mini project report**

| | |
|---|---|
| Student | Shreyansh Pandey |
| Programme | B.Tech, Artificial Intelligence and Machine Learning |
| Roll number | |
| College | |
| Guide | |
| Date | |

---

## Abstract

StreamScope measures which movie genres attract a large number of ratings and which of those genres are thinly stocked. The raw input is the MovieLens 32M collection: 32,000,204 ratings, stored on a single-node HDFS cluster and aggregated with PySpark. The aggregate is a small genre table. A Docker container uploads that table into MongoDB and a Streamlit application queries it to draw comparisons. A second MongoDB collection is reserved for unstructured TMDB watch-provider documents so that subscription coverage in India can be queried the same way. The ratings themselves stay on HDFS. They are not copied into MongoDB.

---

## 1. Problem statement

An OTT catalogue decision needs two facts at once: where audience interest is high, and where the titles actually offered on subscription are few. MovieLens is not an OTT view log. It is a public ratings collection, large enough that a spreadsheet is the wrong tool and a single-node Hadoop cluster is a fair tool. The project therefore treats rating activity as a proxy for interest, and treats TMDB flatrate providers in region IN as a proxy for subscription availability.

The question the system answers is:

> For each genre, how large is the rating activity, how many titles carry that activity, and, once provider documents are stored, what fraction of the studied titles have an India subscription provider?

Scope that keeps the work inside one laptop:

- One machine, one DataNode, replication factor 1. This is not a multi-node cluster.
- MovieLens ratings through 12 October 2023. Not OTT views, not revenue.
- Genre labels are multi-label. A title with two genres is counted in both, so genre totals add up to more than 32,000,204.
- The provider query uses India (`IN`) and flatrate (subscription) providers only.

**Figure 1. Problem framing used on the title slide.**

> Paste screenshot here.

---

## 2. Objective

1. Store the MovieLens files on HDFS and show that the blocks are distributed on the single DataNode.
2. Use PySpark to count the ratings and to aggregate interest by genre.
3. Run the Python dashboard and MongoDB in Docker.
4. Upload the curated genre table into MongoDB and query it for charts and tables.
5. Query unstructured TMDB provider documents from the same database when they are present, and show coverage and gap.

---

## 3. Dataset

Source: GroupLens MovieLens 32M. Cite Harper and Konstan, “The MovieLens Datasets: History and Context,” ACM Transactions on Interactive Intelligent Systems, 2015.

| File | Role | HDFS size | Blocks |
|---|---|---|---|
| ratings.csv | userId, movieId, rating, timestamp | 877,076,222 bytes (about 836.4 MiB) | 7 (six full 128 MiB blocks and one short block) |
| movies.csv | movieId, title, genres | about 4.0 MiB | 1 |
| links.csv | movieId, imdbId, tmdbId | about 1.9 MiB | 1 |

`wc -l` on the local ratings file returned 32,000,205 lines, which is the header plus 32,000,204 ratings. PySpark’s `count()` on `hdfs://localhost:9000/streamscope/raw/movielens/ratings.csv` returned the same 32,000,204.

The study list for providers is the 2,000 MovieLens titles with the most ratings and a non-null `tmdbId`. That list was written by Spark to `/streamscope/curated/catalog_universe` (2,000 data rows plus a header).

**Figure 2. Checksums and line count after the MovieLens download.**

> Paste screenshot here.

---

## 4. Tools and configuration

| Component | Version and setting |
|---|---|
| Host | WSL2 Ubuntu on one laptop. Hadoop is not inside Docker. |
| Java | OpenJDK 11. `JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64` |
| Hadoop | 3.3.6. `HADOOP_HOME=/home/pandey/hadoop` |
| `fs.defaultFS` | `hdfs://localhost:9000` |
| Replication | `dfs.replication=1` |
| Block size | 134,217,728 bytes (128 MiB) |
| NameNode directory | `file:///home/pandey/hadoopdata/namenode` |
| DataNode directory | `file:///home/pandey/hadoopdata/datanode` |
| NameNode UI | http://localhost:9870 |
| Spark | 3.5.9, pre-built for Hadoop 3, Scala 2.12. `master local[*]`. Spark 4 was not used because it needs Java 17. |
| MongoDB | Docker image `mongo:7`, database `streamscope`, port 27017 |
| Dashboard image | `python:3.11-slim` with Streamlit, Plotly, pandas, pymongo, and requests |
| Dashboard URL | http://localhost:8502 |

YARN was started once, so ResourceManager and NodeManager appeared in `jps`, and was then stopped. Later Spark jobs run in local mode so the laptop memory stays available to the driver. The SecondaryNameNode is a checkpoint process, not a standby NameNode.

Security is SIMPLE. There is one live DataNode, at 127.0.0.1:9866.

**Figure 3. `core-site.xml` and `hdfs-site.xml` showing the default filesystem, replication, and storage directories.**

> Paste screenshot here.

---

## 5. Methodology

The pipeline order is the order used in the demo.

```text
MovieLens CSV files
        |
        v
HDFS  /streamscope/raw/movielens/{ratings,movies,links}.csv
        |
        v
PySpark on the host
  count of ratings
  genre aggregate  ->  /streamscope/curated/genre_interest
  top 2000 titles  ->  /streamscope/curated/catalog_universe
        |
        v
hdfs dfs -getmerge of the genre table to ~/streamscope/output/genre_interest.csv
        |
        v
Docker
  streamscope-mongo        stores documents
  streamscope-dashboard    uploads the genre table, then queries MongoDB
        |
        v
Streamlit charts and tables on http://localhost:8502
```

Spark does the aggregation. MongoDB stores the small result and the provider JSON. Plotly only draws. Streamlit does not plot a Spark DataFrame directly. The container cannot use `hdfs://localhost:9000`, because inside a container `localhost` is the container itself, so the curated CSV is mounted in and then inserted into MongoDB.

**Figure 4. Pipeline diagram used in the presentation.**

> Paste screenshot here.

---

## 6. HDFS upload and distribution

The three files were copied with `hdfs dfs -put` into `/streamscope/raw/movielens/`. `hdfs fsck` reported the filesystem HEALTHY. The ratings file occupies 7 blocks at replication 1. `hdfs dfsadmin -report` showed 1 live DataNode and safe mode off. A Spark count from `hdfs://localhost:9000` confirmed the NameNode path is readable by the processing engine, not only by the shell.

NameNode identity recorded from the overview page:

- Cluster ID `CID-bbf9ccdb-443d-4200-af3f-c2e03969d97d`
- Block pool `BP-833120473-127.0.1.1-1792923605495`
- NameNode started 25 October 2026, 15:50:32 IST

**Figure 5. `jps` showing NameNode, DataNode, and SecondaryNameNode.**

> Paste screenshot here.

**Figure 6. `hdfs dfs -ls /streamscope/raw/movielens` with the three CSV files.**

> Paste screenshot here.

**Figure 7. `hdfs fsck` output ending in HEALTHY, with the block count.**

> Paste screenshot here.

**Figure 8. `hdfs dfsadmin -report` showing Live datanodes (1) and safe mode off.**

> Paste screenshot here.

**Figure 9. NameNode UI overview (http://localhost:9870).**

> Paste screenshot here.

**Figure 10. NameNode “Browse the file system” for the ratings file, showing the 7 blocks.**

> Paste screenshot here.

---

## 7. Spark processing

Spark 3.5.9 reads the HDFS CSV files with `HADOOP_CONF_DIR` pointed at the Hadoop configuration. The driver runs as `local[*]`.

The count job prints `RATINGS_ON_HDFS 32000204`.

The genre job joins ratings to movies on `movieId`, splits `genres` on `|`, drops empty labels and `(no genres listed)`, and aggregates per genre:

- `rating_count`: number of ratings
- `rater_count`: distinct `userId`
- `movie_count`: distinct `movieId`
- `avg_rating`: mean rating, rounded to 4 decimal places

The result is written as one CSV directory to `hdfs://localhost:9000/streamscope/curated/genre_interest`. The file contains 20 data rows. One row is a broken title that leaked into the genre column because of a doubled quote in `movies.csv` (“We're Comin' To Get Ya!”). The dashboard keeps an allow-list of the 19 real MovieLens genres and does not plot that row.

The catalogue job keeps titles with a non-null `tmdbId`, orders them by rating count, and writes the top 2,000 rows to `/streamscope/curated/catalog_universe`.

**Figure 11. Spark log ending in `RATINGS_ON_HDFS 32000204`.**

> Paste screenshot here.

**Figure 12. Spark `show()` of the genre aggregate, with Drama at the top.**

> Paste screenshot here.

---

## 8. Results from the genre table

These figures are the PySpark aggregate. Because genres overlap, the rating counts sum to 87,063,719, which is 55,063,515 above the 32,000,204 raw ratings. That gap is double counting, not extra data.

| Genre | Ratings | Raters | Movies | Average | Ratings per movie | Share of genre-rows |
|---|---:|---:|---:|---:|---:|---:|
| Drama | 13,973,271 | 200,914 | 33,152 | 3.6825 | 421.5 | 16.05% |
| Comedy | 11,206,925 | 200,693 | 22,447 | 3.4324 | 499.3 | 12.87% |
| Action | 9,665,213 | 200,208 | 9,296 | 3.4764 | 1,039.7 | 11.10% |
| Thriller | 8,679,464 | 200,168 | 11,555 | 3.5317 | 751.1 | 9.97% |
| Adventure | 7,590,522 | 199,968 | 5,156 | 3.5234 | 1,472.2 | 8.72% |
| Sci-Fi | 5,717,337 | 197,981 | 4,797 | 3.4917 | 1,191.9 | 6.57% |
| Romance | 5,524,615 | 198,689 | 10,048 | 3.5450 | 549.8 | 6.35% |
| Crime | 5,373,051 | 198,878 | 6,704 | 3.6918 | 801.5 | 6.17% |
| Fantasy | 3,702,759 | 193,899 | 3,784 | 3.5122 | 978.5 | 4.25% |
| Children | 2,731,841 | 182,409 | 4,447 | 3.4392 | 614.3 | 3.14% |
| Mystery | 2,615,322 | 188,889 | 3,894 | 3.6731 | 671.6 | 3.00% |
| Horror | 2,492,315 | 174,469 | 8,468 | 3.3072 | 294.3 | 2.86% |
| Animation | 2,214,562 | 173,853 | 4,586 | 3.6153 | 482.9 | 2.54% |
| War | 1,594,110 | 181,626 | 2,225 | 3.7917 | 716.5 | 1.83% |
| IMAX | 1,494,179 | 151,832 | 195 | 3.5933 | 7,662.5 | 1.72% |
| Musical | 1,159,516 | 152,251 | 1,033 | 3.5543 | 1,122.5 | 1.33% |
| Western | 596,654 | 134,707 | 1,489 | 3.6002 | 400.7 | 0.69% |
| Documentary | 427,353 | 74,356 | 9,103 | 3.6912 | 46.9 | 0.49% |
| Film-Noir | 304,710 | 83,671 | 350 | 3.9158 | 870.6 | 0.35% |

Reading the table:

- Drama leads raw activity and catalogue size.
- Film-Noir leads the average (3.9158) on a small catalogue.
- IMAX has the highest ratings per movie (about 7,663) because 195 titles carry 1,494,179 ratings.
- Documentary is the opposite case: 9,103 titles and only about 47 ratings per movie.
- Horror is a large catalogue (8,468 movies) with the lowest average (3.3072).

Ratings per movie is the content-gap signal available before provider data: a high value means a few titles absorb a lot of interest.

**Figure 13. Activity tab: total ratings, share pie, and top-five pie.**

> Paste screenshot here.

**Figure 14. Quality tab: average-rating bar, scatter of activity against average, and rank table.**

> Paste screenshot here.

**Figure 15. Catalogue tab: catalogue pie, ratings per movie, movie counts, distinct raters, and the aggregation table.**

> Paste screenshot here.

**Figure 16. Comparison table with activity rank, rating rank, and demand rank.**

> Paste screenshot here.

---

## 9. MongoDB and unstructured provider documents

MongoDB runs as the container `streamscope-mongo` (image `mongo:7`). The data directory is the named volume `app_mongodata`, created because Compose is started from `~/streamscope/app`. Nothing in this database is a cloud Atlas cluster.

Two collections are used in database `streamscope`:

| Collection | What is stored | How it is queried |
|---|---|---|
| `genre_interest` | One document per genre: `genre`, `rating_count`, `rater_count`, `movie_count`, `avg_rating` | `find` sorted by `rating_count`, and an aggregation that divides ratings by movies |
| `tmdb_providers` | One document per fetched title: MovieLens ids, genres, HTTP status, and the raw TMDB JSON payload | `find` where `http_status` is 200, then read `payload.results.IN.flatrate` |

The genre upload runs when the dashboard container starts (`upload_genres.py`). It replaces `genre_interest` with the 19 allow-listed rows from the PySpark CSV. The aggregation the page runs is:

```javascript
db.genre_interest.aggregate([
  { $match: { movie_count: { $gt: 0 } } },
  { $project: {
      genre: 1, rating_count: 1, movie_count: 1, avg_rating: 1,
      ratings_per_movie: { $divide: ["$rating_count", "$movie_count"] }
  }},
  { $sort: { ratings_per_movie: -1 } }
])
```

Provider documents are unstructured JSON. A title counts as covered when `payload.results.IN.flatrate` is a non-empty list. Gap for a genre is `1 - (covered titles / stored titles)`. A multi-genre title is counted in each of its genres, same as the rating aggregate.

`fetch_tmdb.py` on the host writes those documents. It calls the TMDB watch-provider endpoint for region IN, retries on network failure, and inserts a document only when the HTTP status is 200. The API key is read from the environment variable `TMDB_API_KEY` and is not stored in the image or the Git repository. At the time this report was drafted, that fetch had saved some titles and had not been confirmed complete at 2,000 documents. The coverage tab therefore states that no completed provider set is loaded, instead of inventing a gap percentage.

**Figure 17. `docker exec ... mongosh` count of `genre_interest` (expected 19) and a sample Drama document.**

> Paste screenshot here.

**Figure 18. One `tmdb_providers` document showing the nested `flatrate` provider names. Paste this after the fetch has stored documents. If the count is still 0, paste the count output and say the fetch is incomplete.**

> Paste screenshot here.

**Figure 19. MongoDB coverage tab. If provider documents exist, this is the gap bar, the provider pie, and the coverage table. If they do not, this is the warning the page shows.**

> Paste screenshot here.

---

## 10. Docker

Hadoop and Spark stay on the host. Docker runs two services from `~/streamscope/app/docker-compose.yml`:

- `mongo`: image `mongo:7`, container `streamscope-mongo`, host port 27017, volume `mongodata`.
- `dashboard`: built from the local `Dockerfile`, container `streamscope-dashboard`, host port 8502.

The image is `python:3.11-slim`. It installs Streamlit, Plotly, pandas, pymongo, and requests, and copies `docker_dashboard.py`, `upload_genres.py`, and `entrypoint.sh`. On start, the entrypoint uploads the genre CSV mounted from `../output/genre_interest.csv`, then serves Streamlit on `0.0.0.0:8502`. A healthcheck requests `/_stcore/health`.

The image does not contain the MovieLens files, the Hadoop blocks, Spark, or the TMDB key.

**Figure 20. `docker ps` showing `streamscope-mongo` and `streamscope-dashboard`.**

> Paste screenshot here.

**Figure 21. Container log line `UPLOADED 19 genres into streamscope.genre_interest`.**

> Paste screenshot here.

**Figure 22. Browser address bar on http://localhost:8502 with the four metrics (19 genres stored, Drama as highest activity).**

> Paste screenshot here.

---

## 11. Streamlit interface

The dashboard is one page with a sidebar and five tabs.

- The sidebar selects genres and shows the three MongoDB queries the page runs.
- Activity: bar of rating counts, pie of share, pie of the top five genres against the rest.
- Quality: bar of average rating (axis from 3.0 to 4.0 so small differences stay visible), scatter of activity against average with point size equal to movie count, and a rank table.
- Catalogue: pie of movie counts, bar of ratings per movie, bar of catalogue size, bar of distinct raters, and the aggregation table.
- Comparison table: activity rank, rating rank, and demand rank on one row per genre.
- MongoDB coverage: gap and provider charts when `tmdb_providers` has HTTP 200 documents.

Metrics at the top report how many genres are stored, how many are selected, which selected genre has the most ratings, and how many TMDB titles were queried.

**Figure 23. Sidebar with the genre picker and the query text.**

> Paste screenshot here.

---

## 12. How to run the demo

Hadoop is already up if `jps` shows NameNode and DataNode. Do not format the NameNode.

```bash
sudo service ssh start
start-dfs.sh
jps
hdfs dfsadmin -safemode get
hdfs dfs -ls /streamscope/raw/movielens
sudo service docker start
hdfs dfs -getmerge /streamscope/curated/genre_interest ~/streamscope/output/genre_interest.csv
cd ~/streamscope/app
sudo docker compose up -d --build
sudo docker ps
```

Open http://localhost:8502. Safe mode must be off, the three CSV files must be listed, and both containers must be up.

---

## 13. Limits

- One DataNode and replication 1. The result does not demonstrate multi-node scaling.
- MovieLens ratings are not watches, completion, or revenue.
- Genre totals double-count multi-genre titles.
- One malformed genre string was dropped by the allow-list rather than repaired in the source CSV.
- Provider coverage is only as complete as `tmdb_providers`. A partial fetch must be described as partial.
- The dashboard container reads the curated file and MongoDB. It does not open a Spark session against the NameNode.

---

## 14. Conclusion

The ratings collection is on HDFS, the block report shows a healthy single-node distribution, and PySpark both counted 32,000,204 ratings and produced the genre table. Docker runs MongoDB and the Streamlit application. The application uploads that table and answers the comparison questions with MongoDB `find` and `aggregate` queries. The same query path is ready for the unstructured provider documents. The useful contrast in the results is between raw activity (Drama), average rating (Film-Noir), and ratings per title (IMAX high, Documentary low).

---

## References

1. F. M. Harper and J. A. Konstan, “The MovieLens Datasets: History and Context,” *ACM Transactions on Interactive Intelligent Systems*, vol. 5, no. 4, 2015.
2. Apache Hadoop 3.3.6 documentation. HDFS user guide and `dfsadmin`.
3. Apache Spark 3.5.9 documentation. PySpark SQL.
4. GroupLens, MovieLens 32M dataset, https://grouplens.org/datasets/movielens/.
5. TMDB API, movie watch providers, https://developer.themoviedb.org/reference/movie-watch-providers.

---

## Appendix A. Demo order for the viva

Speak in this order. Each step has a screen already listed above.

1. One sentence of the problem: interest from ratings, availability from subscription providers, gap where interest is high and availability is thin. (Figure 1.)
2. Show HDFS: `ls`, fsck, the NameNode overview, and the 7 blocks. (Figures 6 to 10.)
3. Show the Spark count 32,000,204 and the genre table. (Figures 11 and 12.)
4. Show `docker ps` and http://localhost:8502. (Figures 20 and 22.)
5. Walk Activity, then Quality, then Catalogue, and read one row of the comparison table. Point at Drama, Film-Noir, and IMAX. (Figures 13 to 16.)
6. Show the MongoDB query in the sidebar and the `genre_interest` document. If provider documents are loaded, show the gap chart. If not, say the fetch is incomplete and do not quote a gap percentage. (Figures 17 to 19.)
