import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
DWH_ROLE_ARN= config.get('IAM_ROLE','ARN')
#LOG_DATA= config.get('S3','LOG_DATA')
#LOG_JSONPATH= config.get('S3','LOG_JSONPATH')
SONG_DATA= config.get('S3','SONG_DATA')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplay;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS song;"
artist_table_drop = "DROP TABLE IF EXISTS artist;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events
(
    artist           varchar,
    auth             varchar,
    first_name       varchar,
    gender           varchar,
    item_in_session  varchar,
    last_name        varchar,
    length           numeric(18,0),
    level            varchar,
    location         varchar,
    method           varchar,
    page             varchar,
    registration     numeric(18,0),
    session_id       integer,
    song             varchar,
    status           integer,
    ts               bigint,
    user_agent       varchar,
    user_id          integer
);
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs
(
    num_songs        integer,
    artist_id        varchar,
    artist_latitude  numeric(18,0),
    artist_longitude numeric(18,0),
    artist_location  varchar,    
    artist_name      varchar,
    song_id          varchar,
    title            varchar,
    duration         numeric(18,0),
    year             integer
);
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplay
(
    songplay_id      int          IDENTITY(0,1),
    ts               bigint       NOT NULL,
    user_id          integer      NOT NULL,
    level            varchar,
    song_id          varchar      NOT NULL,
    artist_id        varchar      NOT NULL,
    session_id       integer,
    location         varchar,
    user_agent       varchar,
    PRIMARY KEY(songplay_id)
) DISTKEY(user_id) SORTKEY(user_id);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users
(
    user_id         integer,
    first_name      varchar       NOT NULL,
    last_name       varchar,
    gender          varchar,
    level           varchar,
    PRIMARY KEY(user_id)
) DISTKEY(user_id) SORTKEY(user_id);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS song
(
    song_id         varchar       NOT NULL,
    artist_id       varchar       NOT NULL,
    title           varchar,
    duration        numeric(18,0) NOT NULL,
    year            integer       NOT NULL,
    PRIMARY KEY(song_id)
) DISTKEY(artist_id) SORTKEY(artist_id);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artist
(
    artist_id        varchar,
    artist_name      varchar,
    location         varchar,
    artist_latitude  numeric(18,0),
    artist_longitude numeric(18,0),
    PRIMARY KEY(artist_id)
) DISTKEY(artist_id) SORTKEY(artist_id);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time
(
    ts       bigint,
    hour             integer          NOT NULL,
    day              integer          NOT NULL,
    week             integer          NOT NULL,
    month            integer          NOT NULL,
    year             integer          NOT NULL,
    weekday          integer          NOT NULL,
    PRIMARY KEY(ts)
) DISTSTYLE ALL;
""")

# STAGING TABLES

staging_events_copy = ("""
COPY staging_events
FROM '{}'
credentials 'aws_iam_role={}'
compupdate off region 'us-west-2'
json '{}'
BLANKSASNULL
EMPTYASNULL maxerror 50000;
""").format(config['S3']['LOG_DATA'], config['IAM_ROLE']['ARN'], config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""
COPY staging_songs
FROM '{}'
credentials 'aws_iam_role={}'
compupdate off region 'us-west-2'
format as json 'auto'
BLANKSASNULL
EMPTYASNULL maxerror 50000;
""").format(SONG_DATA, DWH_ROLE_ARN)

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplay(ts,
                     user_id,
                     level,
                     song_id,
                     artist_id,
                     session_id,
                     location,
                     user_agent)
SELECT  staging_events.ts,
        staging_events.user_id,
        staging_events.level,
        staging_songs.song_id,
        staging_songs.artist_id, 
        staging_events.session_id, 
        staging_events.location, 
        staging_events.user_agent 
FROM staging_events JOIN staging_songs ON 
    (staging_events.artist=staging_songs.artist_name AND staging_events.song=staging_songs.title)
WHERE page='NextSong' and user_id is NOT NULL;
""")

user_table_insert = ("""
INSERT INTO users (user_id,
                  first_name, 
                  last_name, 
                  gender, 
                  level)
SELECT DISTINCT user_id,
                first_name,
                last_name,
                gender,
                level 
FROM staging_events
WHERE page = 'NextSong';

""")

song_table_insert = ("""
INSERT INTO song (song_id,
                  artist_id,
                  title,
                  duration,
                  year)
SELECT DISTINCT song_id,
                artist_id,
                title,
                duration,
                year 
FROM staging_songs;
""")

artist_table_insert = ("""
INSERT INTO artist( artist_id,
                    artist_name,
                    location,
                    artist_latitude,
                    artist_longitude)
SELECT DISTINCT artist_id,
                artist_name,
                location,
                artist_latitude,
                artist_longitude 
FROM staging_events
JOIN staging_songs ON
    (staging_events.artist =staging_songs.artist_name)
WHERE page = 'NextSong';
""")

time_table_insert = ("""
INSERT INTO time(ts,
                 hour,
                 day,
                 week,
                 month,
                 year,
                 weekday)
SELECT DISTINCT ts,
                extract(hour from TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second'),
                extract(day from TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second'),
                extract(week from TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second'),
                extract(month from TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second'),
                extract(year from TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second'),
                extract(weekday from TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second')
FROM staging_events;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
