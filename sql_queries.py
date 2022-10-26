import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_events
    (
        artist          TEXT,
        auth            TEXT,
        first_name      TEXT,
        gender          TEXT,
        item_in_session INTEGER,
        last_name       TEXT,
        length          FLOAT4,
        level           TEXT,
        location        TEXT,
        method          TEXT,
        page            TEXT,
        registration    FLOAT8,
        session_id      INTEGER,
        song            TEXT,
        status          INTEGER,
        ts              BIGINT,
        user_agent      TEXT,
        user_id         TEXT
    );
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs (
    song_id             TEXT,
    title               TEXT,
    duration            FLOAT4,
    year                SMALLINT,
    artist_id           TEXT,
    artist_name         TEXT,
    artist_latitude     REAL,
    artist_longitude    REAL,
    artist_location     TEXT,
    num_songs           INTEGER
);
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays
    (
        songplay_id    BIGINT IDENTITY(1, 1) PRIMARY KEY,
        start_time     TIMESTAMP NOT NULL SORTKEY,
        user_id        TEXT NOT NULL DISTKEY,
        level          TEXT,
        song_id        TEXT,
        artist_id      TEXT,
        session_id     INTEGER,
        location       TEXT,
        user_agent     TEXT
    ) diststyle key;
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users
    (
        user_id     TEXT PRIMARY KEY SORTKEY,
        first_name  TEXT,
        last_name   TEXT,
        gender      TEXT,
        level       TEXT
    ) diststyle all;
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs
    (
        song_id     TEXT PRIMARY KEY SORTKEY,
        title       TEXT,
        artist_id   TEXT DISTKEY,
        year        SMALLINT,
        duration    FLOAT4
    ) diststyle key;
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists
    (
        artist_id   TEXT PRIMARY KEY SORTKEY,
        name        TEXT,
        location    TEXT,
        latitude    FLOAT4,
        longitude   FLOAT4
    ) diststyle all;
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time
    (
        start_time  TIMESTAMP PRIMARY KEY SORTKEY,
        hour        SMALLINT,
        day         SMALLINT,
        week        SMALLINT,
        month       SMALLINT,
        year        SMALLINT DISTKEY,
        weekday     SMALLINT
    ) diststyle key;
""")

# STAGING TABLES

staging_events_copy = ("""copy staging_events
                          from {}
                          iam_role {}
                          json {};
                       """).format(config.get('S3','LOG_DATA'), config.get('IAM_ROLE', 'ARN'), config.get('S3','LOG_JSONPATH'))

staging_songs_copy = ("""
COPY staging_songs
FROM {0}
iam_role {1}
region 'us-west-2'
json 'auto'
""").format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'])

# FINAL TABLES

songplay_table_insert = ("""
   INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
   SELECT DISTINCT timestamp 'epoch' + ts / 1000 * interval '1 second' AS start_time,
   e.user_id,
   e.level,
   s.song_id,
   s.artist_id,
   e.session_id,
   e.location,
   e.user_agent
   FROM staging_events e, staging_songs s
   WHERE e.song = s.title
   AND e.artist = s.artist_name
   AND page = 'NextSong';
""")

user_table_insert = ("""
    INSERT INTO users
    SELECT user_id,
    first_name,
    last_name,
    gender,
    level
    FROM staging_events
    WHERE user_id IS NOT null;
""")

song_table_insert = ("""
    INSERT INTO songs
    SELECT DISTINCT song_id,
    title,
    artist_id,
    year,
    duration
    FROM staging_songs
    WHERE song_id IS NOT NULL;

""")

artist_table_insert = ("""
    INSERT INTO artists
    SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
    FROM staging_songs
    WHERE artist_id IS NOT NULL;
""")

time_table_insert = ("""
    INSERT INTO time
    SELECT DISTINCT timestamp 'epoch' + ts / 1000 * interval '1 second' AS start_time,
                extract(hour from (timestamp 'epoch' + ts / 1000 * interval '1 second')),
                extract(day from (timestamp 'epoch' + ts / 1000 * interval '1 second')),
                extract(week from (timestamp 'epoch' + ts / 1000 * interval '1 second')),
                extract(month from (timestamp 'epoch' + ts / 1000 * interval '1 second')),
                extract(year from (timestamp 'epoch' + ts / 1000 * interval '1 second')),
                extract(weekday from (timestamp 'epoch' + ts / 1000 * interval '1 second'))
           FROM staging_events
          WHERE ts IS NOT NULL;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop,song_table_drop,artist_table_drop, time_table_drop]

copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
