import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

ARN = config.get('IAM_ROLE', 'ARN')
SONG_DATA = config.get('S3', 'SONG_DATA')
LOG_DATA = config.get('S3', 'LOG_DATA')
LOG_JSON_PATH = config.get('S3', 'LOG_JSON_PATH')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create = ("""
CREATE TABLE staging_events(
    artist VARCHAR,
    auth VARCHAR,
    first_name VARCHAR,
    gender VARCHAR,
    item_in_section INT,
    last_name VARCHAR,
    length FLOAT,
    level VARCHAR,
    location VARCHAR,
    method VARCHAR,
    page VARCHAR,
    registration FLOAT,
    session_id INT,
    song VARCHAR,
    status INT,
    ts BIGINT,
    user_agent VARCHAR,
    user_id INT
)
""")

staging_songs_table_create = ("""
CREATE TABLE staging_songs(
    artist_id VARCHAR(max),
    artist_latitude FLOAT,
    artist_location VARCHAR(max),
    artist_longitude FLOAT,
    artist_name VARCHAR(max),
    duration FLOAT,
    num_songs INT,
    song_id VARCHAR,
    title VARCHAR(max),
    year INT
)
""")

songplay_table_create = ("""
CREATE TABLE songplays(
    songplay_id BIGINT IDENTITY(1, 1) NOT NULL PRIMARY KEY,
    start_time TIMESTAMP NOT NULL,
    user_id INT NOT NULL,
    level VARCHAR NOT NULL,
    song_id VARCHAR NOT NULL,
    artist_id VARCHAR,
    session_id INT,
    location VARCHAR,
    user_agent VARCHAR
)
""")

user_table_create = ("""
CREATE TABLE users(
    user_id INT NOT NULL PRIMARY KEY,
    first_name VARCHAR,
    last_name VARCHAR,
    gender VARCHAR,
    level VARCHAR NOT NULL
)
""")

song_table_create = ("""
CREATE TABLE songs(
    song_id VARCHAR NOT NULL PRIMARY KEY,
    title VARCHAR(max) NOT NULL,
    artist_id VARCHAR,
    year INT,
    duration FLOAT
)
""")

artist_table_create = ("""
CREATE TABLE artists(
    artist_id VARCHAR NOT NULL PRIMARY KEY,
    name VARCHAR(max) NOT NULL,
    location VARCHAR(max),
    latitude FLOAT,
    longitude FLOAT
)
""")

time_table_create = ("""
CREATE TABLE time(
    start_time TIMESTAMP NOT NULL PRIMARY KEY,
    hour INT,
    day INT,
    week INT,
    month INT,
    year INT,
    weekday INT
)
""")

# STAGING TABLES

staging_events_copy = ("""
COPY staging_events
FROM {}
CREDENTIALS 'aws_iam_role={}'
FORMAT AS JSON {}
REGION 'us-west-2'
BLANKSASNULL
EMPTYASNULL
""").format(LOG_DATA, ARN, LOG_JSON_PATH)

staging_songs_copy = ("""
COPY staging_songs
FROM {}
CREDENTIALS 'aws_iam_role={}'
FORMAT AS JSON 'auto'
REGION 'us-west-2'
BLANKSASNULL
EMPTYASNULL
""").format(SONG_DATA, ARN)

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays(start_time, user_id, level, song_id, artist_id,
session_id, location, user_agent)
SELECT
TIMESTAMP 'epoch' + ts::numeric / 1000 * INTERVAL '1 second' AS start_time,
user_id, level, s.song_id, s.artist_id, session_id, location, user_agent
FROM staging_events e
JOIN staging_songs s
ON (s.title = e.song) AND (e.artist = s.artist_name)
WHERE (ts IS NOT NULL) AND (user_id IS NOT NULL)
      AND (level is NOT NULL) AND (song_id IS NOT NULL)
""")

user_table_insert = ("""
INSERT INTO users
SELECT user_id, first_name, last_name, gender, level
FROM staging_events
WHERE (user_id, ts) IN (
  SELECT user_id, MAX(ts)
  FROM staging_events
  WHERE user_id IS NOT NULL
  GROUP BY user_id) AND (level IS NOT NULL)
""")

song_table_insert = ("""
INSERT INTO songs
SELECT DISTINCT song_id, title, artist_id, year, duration
FROM staging_songs
WHERE (song_id IS NOT NULL) AND (title IS NOT NULL)
""")

artist_table_insert = ("""
INSERT INTO artists
SELECT DISTINCT artist_id, artist_name, artist_location,
                artist_latitude, artist_longitude
FROM staging_songs
WHERE (artist_id IS NOT NULL) AND (artist_name IS NOT NULL)
""")

time_table_insert = ("""
INSERT INTO time
SELECT DISTINCT
TIMESTAMP 'epoch' + ts::numeric / 1000 * INTERVAL '1 second' AS start_time,
DATE_PART(h, start_time) as hour,
DATE_PART(d, start_time) as day,
DATE_PART(w, start_time) as week,
DATE_PART(mon, start_time) as month,
DATE_PART(y, start_time) as year,
DATE_PART(weekday, start_time) as weekday
FROM staging_events
WHERE ts IS NOT NULL
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create,
                        staging_songs_table_create, songplay_table_create,
                        user_table_create, song_table_create,
                        artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop,
                      songplay_table_drop, user_table_drop, song_table_drop,
                      artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert,
                        song_table_insert, artist_table_insert,
                        time_table_insert]
