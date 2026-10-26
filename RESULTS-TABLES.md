# StreamScope — measured result tables

All genre numbers come from PySpark over MovieLens 32M on HDFS (through 12 Oct 2023).  
Multi-genre titles are counted in **each** genre, so genre `rating_count` sums to **87,063,719**, not 32,000,204.

---

## Table 1. HDFS storage summary

| Item | Value |
|------|------:|
| Default FS | `hdfs://localhost:9000` |
| Replication | 1 |
| Block size | 134,217,728 bytes (128 MiB) |
| Live DataNodes | 1 (127.0.0.1:9866) |
| Safe mode | OFF |
| Configured capacity | ~1006.85 GB |
| DFS used (cluster) | ~848.96–849.02 MB |
| Total blocks (3 files) | 9 |
| fsck status (ratings.csv) | HEALTHY |

| File | HDFS path | Size (bytes) | Blocks |
|------|-----------|-------------:|-------:|
| ratings.csv | `/streamscope/raw/movielens/ratings.csv` | 877,076,222 | 7 |
| movies.csv | `/streamscope/raw/movielens/movies.csv` | ~4.0 M | 1 |
| links.csv | `/streamscope/raw/movielens/links.csv` | ~1.9 M | 1 |

---

## Table 2. Spark validation — raw ratings

| Check | Result |
|-------|-------:|
| Local `wc -l` ratings.csv | 32,000,205 (header + 32,000,204 ratings) |
| PySpark `count()` on HDFS ratings | **32,000,204** |
| Job exit code | 0 |

---

## Table 3. Genre aggregate (PySpark → HDFS → MongoDB `genre_interest`)

Sorted by `rating_count`. Broken CSV row excluded from dashboard (19 genres kept).

| Rank | Genre | rating_count | rater_count | movie_count | avg_rating | ratings_per_movie | activity_share %* |
|-----:|-------|-------------:|------------:|------------:|-----------:|------------------:|------------------:|
| 1 | Drama | 13,973,271 | 200,914 | 33,152 | 3.6825 | 421.5 | 16.05 |
| 2 | Comedy | 11,206,925 | 200,693 | 22,447 | 3.4324 | 499.3 | 12.87 |
| 3 | Action | 9,665,213 | 200,208 | 9,296 | 3.4764 | 1,039.7 | 11.10 |
| 4 | Thriller | 8,679,464 | 200,168 | 11,555 | 3.5317 | 751.1 | 9.97 |
| 5 | Adventure | 7,590,522 | 199,968 | 5,156 | 3.5234 | 1,472.2 | 8.72 |
| 6 | Sci-Fi | 5,717,337 | 197,981 | 4,797 | 3.4917 | 1,191.9 | 6.57 |
| 7 | Romance | 5,524,615 | 198,689 | 10,048 | 3.5450 | 549.8 | 6.35 |
| 8 | Crime | 5,373,051 | 198,878 | 6,704 | 3.6918 | 801.5 | 6.17 |
| 9 | Fantasy | 3,702,759 | 193,899 | 3,784 | 3.5122 | 978.5 | 4.25 |
| 10 | Children | 2,731,841 | 182,409 | 4,447 | 3.4392 | 614.3 | 3.14 |
| 11 | Mystery | 2,615,322 | 188,889 | 3,894 | 3.6731 | 671.6 | 3.00 |
| 12 | Horror | 2,492,315 | 174,469 | 8,468 | 3.3072 | 294.3 | 2.86 |
| 13 | Animation | 2,214,562 | 173,853 | 4,586 | 3.6153 | 482.9 | 2.54 |
| 14 | War | 1,594,110 | 181,626 | 2,225 | 3.7917 | 716.5 | 1.83 |
| 15 | IMAX | 1,494,179 | 151,832 | 195 | 3.5933 | 7,662.5 | 1.72 |
| 16 | Musical | 1,159,516 | 152,251 | 1,033 | 3.5543 | 1,122.5 | 1.33 |
| 17 | Western | 596,654 | 134,707 | 1,489 | 3.6002 | 400.7 | 0.69 |
| 18 | Documentary | 427,353 | 74,356 | 9,103 | 3.6912 | 46.9 | 0.49 |
| 19 | Film-Noir | 304,710 | 83,671 | 350 | 3.9158 | 870.6 | 0.35 |

\* Share = `rating_count / 87,063,719` (genre-row total, not raw 32M).

**Excluded junk row (not a genre):** `"We're Comin' To Get Ya!"" (2014)"` — 1 rating, avg 2.0.

---

## Table 4. Rankings used for “content gap” (MovieLens side)

| Metric | Top genre | Value |
|--------|-----------|------:|
| Highest total activity | Drama | 13,973,271 ratings |
| Largest catalogue | Drama | 33,152 movies |
| Highest average rating | Film-Noir | 3.9158 |
| Lowest average rating (major genres) | Horror | 3.3072 |
| Highest ratings per movie | IMAX | 7,662.5 |
| Lowest ratings per movie | Documentary | 46.9 |

---

## Table 5. MongoDB startup query (`run_queries.py` / `query_runs`)

After container start (genre collection populated):

| Field | Measured value |
|-------|----------------|
| `genre_documents` | 19 |
| `highest_activity` | Drama |
| `highest_activity_ratings` | 13,973,271 |
| `highest_ratings_per_movie` | IMAX |
| `ratings_per_movie` | 7662.5 |
| `tmdb_http_200` | 0 until `fetch_tmdb.py` completes |

**Top 5 by MongoDB aggregation (`ratings_per_movie` descending):**

| Rank | Genre | ratings_per_movie | rating_count |
|-----:|-------|------------------:|-------------:|
| 1 | IMAX | 7662.5 | 1,494,179 |
| 2 | Adventure | 1472.2 | 7,590,522 |
| 3 | Sci-Fi | 1191.9 | 5,717,337 |
| 4 | Musical | 1122.5 | 1,159,516 |
| 5 | Action | 1039.7 | 9,665,213 |

---

## Table 6. Catalogue universe (Spark job for TMDB)

| Item | Value |
|------|------:|
| HDFS path | `/streamscope/curated/catalog_universe` |
| Local merged lines (`wc -l`) | 2,001 |
| Data rows (titles) | 2,000 |
| Rule | Top 2,000 by rating count, `tmdbId` not null |

---

## Table 7. OTT coverage / gap (TMDB → `tmdb_providers`)

**Fill this after fetch completes.** Target: ~2,000 documents with `http_status: 200`.

| Genre | titles_stored | on_ott (IN flatrate) | coverage % | gap % |
|-------|-------------:|---------------------:|-----------:|------:|
| *paste from dashboard or aggregate* | | | | |

**Formulas:**

```text
coverage = on_ott / titles_stored
gap      = (1 - coverage) × 100
```

---

## Table 8. Software versions (environment)

| Component | Version / setting |
|-----------|-------------------|
| Java | OpenJDK 11.0.32 |
| Hadoop | 3.3.6 |
| Spark | 3.5.9 (bin-hadoop3), Scala 2.12.18 |
| MongoDB (Docker) | mongo:7 |
| Dashboard Python | 3.11-slim image |
| Streamlit port (Docker) | 8502 |
