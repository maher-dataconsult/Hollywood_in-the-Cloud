CREATE TABLE tmdb_analytics_db.unified_tmdb_movies
WITH (
    format = 'PARQUET',
    external_location = 's3://tmdb-data-071125/tmdb/curated/unified_tmdb_movies/'
) AS

-- 1. Union top_rated and upcoming into a normalized base
WITH base_movies AS (
    SELECT DISTINCT
        CAST(id AS BIGINT)            AS movie_id,
        title,
        CAST(release_date AS VARCHAR) AS release_date,
        CAST(popularity AS DOUBLE)    AS popularity,
        CAST(vote_average AS DOUBLE)  AS vote_average,
        CAST(vote_count AS BIGINT)    AS vote_count,
        original_language,
        CAST(genre_ids AS VARCHAR)    AS genre_ids
    FROM tmdb_analytics_db.top_rated

    UNION ALL

    SELECT DISTINCT
        CAST(id AS BIGINT)            AS movie_id,
        title,
        CAST(release_date AS VARCHAR) AS release_date,
        CAST(popularity AS DOUBLE)    AS popularity,
        CAST(vote_average AS DOUBLE)  AS vote_average,
        CAST(vote_count AS BIGINT)    AS vote_count,
        original_language,
        CAST(genre_ids AS VARCHAR)    AS genre_ids
    FROM tmdb_analytics_db.upcoming
),

-- 2. Normalize genre_ids: clean spaces and trailing commas
normalized AS (
    SELECT
        movie_id,
        title,
        release_date,
        popularity,
        vote_average,
        vote_count,
        original_language,
        REGEXP_REPLACE(
            REGEXP_REPLACE(
                COALESCE(TRIM(genre_ids), ''),
                '\\s*,\\s*', ','
            ),
            ',$', ''
        ) AS genre_ids_raw
    FROM base_movies
),

-- 3. Explode genre_ids into one row per (movie_id, genre_id_str)
exploded AS (
    SELECT
        n.movie_id,
        n.title,
        n.release_date,
        n.popularity,
        n.vote_average,
        n.vote_count,
        n.original_language,
        TRIM(gid) AS genre_id_str
    FROM normalized n
    CROSS JOIN UNNEST(
        CASE
            WHEN n.genre_ids_raw IS NULL OR n.genre_ids_raw = ''
                THEN ARRAY['']
            ELSE split(n.genre_ids_raw, ',')
        END
    ) AS t(gid)
),

-- 4. Filter to valid numeric genre IDs
valid_genres AS (
    SELECT
        movie_id,
        title,
        release_date,
        popularity,
        vote_average,
        vote_count,
        original_language,
        CAST(genre_id_str AS BIGINT) AS genre_id
    FROM exploded
    WHERE genre_id_str <> ''
      AND regexp_like(genre_id_str, '^[0-9]+$')
),

-- 5. Join with genres to get genre_name
joined AS (
    SELECT DISTINCT
        v.movie_id,
        v.title,
        v.release_date,
        v.popularity,
        v.vote_average,
        v.vote_count,
        v.original_language,
        g.name AS genre_name
    FROM valid_genres v
    LEFT JOIN tmdb_analytics_db.genres g
        ON v.genre_id = g.id
)

-- 6. Final output:
--    One row per movie-genre pair; movies with no valid genre_name will be dropped.
SELECT
    movie_id,
    title,
    release_date,
    popularity,
    vote_average,
    vote_count,
    original_language,
    genre_name
FROM joined
WHERE genre_name IS NOT NULL;
