import os
import json
import csv
import logging
import urllib.request
import urllib.parse
import boto3
from io import StringIO

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# S3 configuration
S3_BUCKET = "tmdb-data-071125"

# Dedicated prefixes for each dataset
TOP_RATED_KEY = "tmdb/raw/top_rated/top_rated_movies.csv"
UPCOMING_KEY = "tmdb/raw/upcoming/upcoming_movies.csv"
GENRES_KEY = "tmdb/raw/genres/movie_genres.csv"

# TMDB configuration
TMDB_BASE_URL = "https://api.themoviedb.org/3"
TMDB_API_TOKEN = os.getenv("TMDB_API_TOKEN")

# AWS client
s3_client = boto3.client("s3")


def _make_tmdb_request(endpoint: str, params: dict | None = None) -> dict:
    """
    Perform a GET request to TMDB using urllib with Bearer token auth.
    """
    if not TMDB_API_TOKEN:
        logger.error("TMDB_API_TOKEN environment variable is not set")
        raise RuntimeError("TMDB_API_TOKEN environment variable is required")

    if params is None:
        params = {}

    query = urllib.parse.urlencode(params)
    url = f"{TMDB_BASE_URL}{endpoint}"
    if query:
        url = f"{url}?{query}"

    logger.info(f"Requesting TMDB URL: {url}")

    headers = {
        "Authorization": f"Bearer {TMDB_API_TOKEN}",
        "Accept": "application/json"
    }

    request = urllib.request.Request(url, headers=headers, method="GET")

    try:
        with urllib.request.urlopen(request) as response:
            status_code = response.getcode()
            body = response.read().decode("utf-8")
    except Exception as e:
        logger.error(f"HTTP request to TMDB failed: {e}")
        raise

    if status_code < 200 or status_code >= 300:
        logger.error(f"TMDB API returned status {status_code}: {body}")
        raise RuntimeError(f"TMDB API error {status_code}")

    try:
        data = json.loads(body)
    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse TMDB response as JSON: {e}, body: {body}")
        raise

    return data


def fetch_all_pages(endpoint: str, extra_params: dict | None = None, max_pages: int | None = None) -> list:
    """
    Fetch all (or up to max_pages) paginated results from a TMDB endpoint.
    Enforces TMDB max pages (500) to avoid 400 errors.
    """
    if extra_params is None:
        extra_params = {}

    page = 1
    all_results: list = []

    while True:
        params = dict(extra_params)
        params["page"] = page

        data = _make_tmdb_request(endpoint, params)
        results = data.get("results", [])
        total_pages = data.get("total_pages", 1)

        # Enforce TMDB cap at 500 pages
        if total_pages > 500:
            logger.warning(f"{endpoint}: total_pages {total_pages} > 500, capping at 500")
            total_pages = 500

        logger.info(
            f"Fetched page {page} of {total_pages} from {endpoint}, items: {len(results)}"
        )

        all_results.extend(results)

        if max_pages is not None and page >= max_pages:
            break
        if page >= total_pages:
            break

        page += 1

    logger.info(f"Total items fetched from {endpoint}: {len(all_results)}")
    return all_results


def fetch_genres() -> list:
    """
    Fetch movie genres from TMDB.
    """
    data = _make_tmdb_request("/genre/movie/list", {"language": "en-US"})
    genres = data.get("genres", [])
    logger.info(f"Fetched {len(genres)} genres")
    return genres


def movie_to_row(movie: dict) -> list:
    """
    Map TMDB movie JSON object to a CSV row for our curated schema.
    Excludes original_title and overview by design.
    """
    # genre_ids as comma-separated string
    genre_ids = movie.get("genre_ids", [])
    if isinstance(genre_ids, list):
        genre_ids_str = ",".join(str(g) for g in genre_ids)
    elif genre_ids is None:
        genre_ids_str = ""
    else:
        genre_ids_str = str(genre_ids)

    return [
        movie.get("id", ""),
        movie.get("title", ""),
        movie.get("release_date", ""),
        movie.get("vote_average", ""),
        movie.get("vote_count", ""),
        movie.get("popularity", ""),
        movie.get("original_language", ""),
        genre_ids_str,
        str(movie.get("adult", "")),
        str(movie.get("video", ""))
    ]


def write_csv_to_s3(s3_key: str, header: list, rows: list[list]):
    """
    Write CSV (header + rows) to S3 at the given key.
    """
    logger.info(f"Writing CSV to s3://{S3_BUCKET}/{s3_key} with {len(rows)} rows")

    csv_buffer = StringIO()
    writer = csv.writer(csv_buffer)

    writer.writerow(header)
    for row in rows:
        writer.writerow(row)

    s3_client.put_object(
        Bucket=S3_BUCKET,
        Key=s3_key,
        Body=csv_buffer.getvalue().encode("utf-8"),
        ContentType="text/csv"
    )

    logger.info(f"Successfully wrote s3://{S3_BUCKET}/{s3_key}")


def lambda_handler(event, context):
    """
    AWS Lambda entrypoint.

    Writes:
      - s3://tmdb-data-071125/tmdb/raw/top_rated/top_rated_movies.csv
      - s3://tmdb-data-071125/tmdb/raw/upcoming/upcoming_movies.csv
      - s3://tmdb-data-071125/tmdb/raw/genres/movie_genres.csv
    with stable, crawler-friendly schemas.
    """
    logger.info("TMDB Lambda execution started")

    # 1. Fetch from TMDB
    top_rated_movies = fetch_all_pages("/movie/top_rated", {"language": "en-US"})
    upcoming_movies = fetch_all_pages("/movie/upcoming", {"language": "en-US"})
    genres = fetch_genres()

    # 2. Define headers (no original_title, no overview)
    movie_header = [
        "id",
        "title",
        "release_date",
        "vote_average",
        "vote_count",
        "popularity",
        "original_language",
        "genre_ids",
        "adult",
        "video"
    ]

    # 3. Transform movies to rows
    top_rated_rows = [movie_to_row(m) for m in top_rated_movies]
    upcoming_rows = [movie_to_row(m) for m in upcoming_movies]

    # 4. Genres CSV
    genres_header = ["id", "name"]
    genres_rows = [[g.get("id", ""), g.get("name", "")] for g in genres]

    # 5. Write to S3
    write_csv_to_s3(TOP_RATED_KEY, movie_header, top_rated_rows)
    write_csv_to_s3(UPCOMING_KEY, movie_header, upcoming_rows)
    write_csv_to_s3(GENRES_KEY, genres_header, genres_rows)

    logger.info("TMDB Lambda execution completed successfully")

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": "TMDB data fetched and stored in S3 successfully",
            "bucket": S3_BUCKET,
            "keys": [TOP_RATED_KEY, UPCOMING_KEY, GENRES_KEY],
            "top_rated_count": len(top_rated_rows),
            "upcoming_count": len(upcoming_rows),
            "genres_count": len(genres_rows),
        })
    }
