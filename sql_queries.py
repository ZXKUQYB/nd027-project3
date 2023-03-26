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

# Manually assign DISTSTYLE when creating a new staging table to avoid COPY ANALYZE from being executed before COPY command
# https://aws.amazon.com/premiumsupport/knowledge-center/redshift-fix-copy-analyze-statupdate-off/
# https://docs.aws.amazon.com/redshift/latest/dg/c_choosing_dist_sort.html
# Google search "redshift copy analyze takes long time"

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events (
  artist VARCHAR, 
  auth VARCHAR, 
  first_name VARCHAR, 
  gender CHAR, 
  item_in_session INT, 
  last_name VARCHAR, 
  length NUMERIC(9,5), 
  level CHAR(4), 
  location VARCHAR, 
  method VARCHAR, 
  page VARCHAR, 
  registration BIGINT, 
  session_id INT, 
  song VARCHAR, 
  status INT, 
  ts BIGINT, 
  user_agent VARCHAR, 
  user_id INT
  ) DISTSTYLE EVEN;
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs (
  artist_id VARCHAR, 
  artist_latitude NUMERIC(8,5), 
  artist_location VARCHAR, 
  artist_longitude NUMERIC(8,5), 
  artist_name VARCHAR, 
  duration NUMERIC(9,5), 
  num_songs INT, 
  song_id VARCHAR, 
  title VARCHAR, 
  year INT
  ) DISTSTYLE EVEN;
""")

# Although the table creation process of the final tables is essentially the same as the Project 1 (using PostgreSQL),
# beware that the Redshift does not enforce table constraints for data manipulation actions. And because of that, 
# the same data manipulation actions in Project 1 should be modified accordingly before applying them to this project.
# https://stackoverflow.com/questions/15170701/amazon-redshift-keys-are-not-enforced-how-to-prevent-duplicate-data
# https://stackoverflow.com/questions/69135667/unique-key-primary-key-in-aws-redshift-table

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
  songplay_id INT IDENTITY(0,1), 
  start_time TIMESTAMP, 
  user_id INT, 
  level CHAR(4) NOT NULL, 
  song_id VARCHAR, 
  artist_id VARCHAR, 
  session_id INT NOT NULL, 
  location VARCHAR, 
  user_agent VARCHAR, 
  CONSTRAINT pk_songplays PRIMARY KEY(songplay_id)
  );
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
  user_id INT, 
  first_name VARCHAR NOT NULL, 
  last_name VARCHAR NOT NULL, 
  gender CHAR NOT NULL, 
  level CHAR(4) NOT NULL, 
  CONSTRAINT pk_users PRIMARY KEY(user_id)
  );
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
  song_id VARCHAR, 
  title VARCHAR NOT NULL, 
  artist_id VARCHAR NOT NULL, 
  year INT, 
  duration NUMERIC(9,5), 
  CONSTRAINT pk_songs PRIMARY KEY(song_id)
  );
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
  artist_id VARCHAR, 
  name VARCHAR NOT NULL, 
  location VARCHAR, 
  latitude NUMERIC(8,5),
  longitude NUMERIC(8,5),
  CONSTRAINT pk_artists PRIMARY KEY(artist_id)
  );
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
  start_time TIMESTAMP, 
  hour INT, 
  day INT, 
  week INT, 
  month INT, 
  year INT, 
  weekday INT, 
  CONSTRAINT pk_time PRIMARY KEY(start_time)
  );
""")

# STAGING TABLES

# https://docs.aws.amazon.com/redshift/latest/dg/r_COPY.html
# https://docs.aws.amazon.com/redshift/latest/dg/t_loading-tables-from-s3.html
# https://www.zuar.com/blog/load-amazon-s3-data-to-redshift/
# https://docs.aws.amazon.com/redshift/latest/dg/copy-usage_notes-copy-from-json.html
# https://docs.aws.amazon.com/redshift/latest/dg/r_COPY_command_examples.html#r_COPY_command_examples-copy-from-json
# https://hevodata.com/learn/json-to-redshift/
# Google search "redshift copy into", "s3 copy json redshift"

staging_events_copy = ("""
COPY staging_events FROM {} 
CREDENTIALS {} 
REGION 'us-west-2' 
JSON {};
""").format(config.get('S3','LOG_DATA'), config.get('IAM_ROLE','ARN'), config.get('S3','LOG_JSONPATH'))

staging_songs_copy = ("""
COPY staging_songs FROM {} 
CREDENTIALS {} 
REGION 'us-west-2' 
JSON 'auto';
""").format(config.get('S3','SONG_DATA'), config.get('IAM_ROLE','ARN'))

# FINAL TABLES

# There is no ON CONFLICT statement in the SQL commands supported by the Redshift. 
# Besides, the primary key constraints are not enforced by Redshift either.
# We will have to execute some functionally equivalent SQL commands to load data into the final tables.
#
# The original data manipulation actions in Project 1 will be changed to 2 separate SQL statements for each table :
#
#    Step 1 - INSERT only the unique values of the PK column into the final table. If there are other columns being declared as NOT NULL in the final table, they will be filled with some dummy data to make the INSERT statement execution successful.
#    Step 2 - UPDATE the final table to populate the remaining columns with data in the staging tables. Only the table rows with a unique PK value already populated in the final table need to be updated in this step.
#
# https://docs.aws.amazon.com/redshift/latest/dg/t_updating-inserting-using-staging-tables-.html
# Google search "redshift insert on conflict"

songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) (
  SELECT DISTINCT timestamp 'epoch' + se.ts/1000 * interval '1 second', 
  se.user_id, 
  se.level, 
  ss.song_id, 
  ss.artist_id, 
  se.session_id, 
  se.location, 
  se.user_agent
  FROM staging_events se
  LEFT OUTER JOIN staging_songs ss ON se.song = ss.title AND se.artist = ss.artist_name AND se.length = ss.duration 
  WHERE se.page = 'NextSong'
  );
""")

# Sort the result set in the UPDATE statement to make sure the latest free/paid level is being reflected in the final table
user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level) (SELECT DISTINCT user_id, 'dummy', 'dummy', 'X', 'XXXX' FROM staging_events WHERE page = 'NextSong');
UPDATE users SET 
first_name = staging_events_sorted.first_name, 
last_name  = staging_events_sorted.last_name, 
gender     = staging_events_sorted.gender, 
level      = staging_events_sorted.level 
FROM (SELECT * FROM staging_events WHERE page = 'NextSong' ORDER BY ts ASC) staging_events_sorted 
WHERE users.user_id = staging_events_sorted.user_id;
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id) (SELECT DISTINCT song_id, 'dummy', 'dummy' FROM staging_songs);
UPDATE songs SET 
title     = staging_songs.title, 
artist_id = staging_songs.artist_id, 
year      = staging_songs.year, 
duration  = staging_songs.duration
FROM staging_songs 
WHERE songs.song_id = staging_songs.song_id;
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, name) (SELECT DISTINCT artist_id, 'dummy' FROM staging_songs);
UPDATE artists SET 
name      = staging_songs.artist_name, 
location  = staging_songs.artist_location, 
latitude  = staging_songs.artist_latitude, 
longitude = staging_songs.artist_longitude 
FROM staging_songs 
WHERE artists.artist_id = staging_songs.artist_id;
""")

time_table_insert = ("""
INSERT INTO time (start_time) (SELECT DISTINCT timestamp 'epoch' + ts/1000 * interval '1 second' FROM staging_events WHERE page = 'NextSong');
UPDATE time SET 
hour    = EXTRACT(hour from timestamp 'epoch' + staging_events.ts/1000 * interval '1 second'), 
day     = EXTRACT(day from timestamp 'epoch' + staging_events.ts/1000 * interval '1 second'), 
week    = EXTRACT(week from timestamp 'epoch' + staging_events.ts/1000 * interval '1 second'), 
month   = EXTRACT(month from timestamp 'epoch' + staging_events.ts/1000 * interval '1 second'), 
year    = EXTRACT(year from timestamp 'epoch' + staging_events.ts/1000 * interval '1 second'), 
weekday = EXTRACT(weekday from timestamp 'epoch' + staging_events.ts/1000 * interval '1 second') 
FROM staging_events 
WHERE time.start_time = timestamp 'epoch' + staging_events.ts/1000 * interval '1 second' AND staging_events.page = 'NextSong';
""")

time_table_insert_original = ("""
INSERT INTO time (
  SELECT DISTINCT timestamp 'epoch' + ts/1000 * interval '1 second', 
  EXTRACT(hour from timestamp 'epoch' + ts/1000 * interval '1 second'), 
  EXTRACT(day from timestamp 'epoch' + ts/1000 * interval '1 second'), 
  EXTRACT(week from timestamp 'epoch' + ts/1000 * interval '1 second'), 
  EXTRACT(month from timestamp 'epoch' + ts/1000 * interval '1 second'), 
  EXTRACT(year from timestamp 'epoch' + ts/1000 * interval '1 second'), 
  EXTRACT(weekday from timestamp 'epoch' + ts/1000 * interval '1 second') 
  FROM staging_events 
  WHERE page = 'NextSong'
  );
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
