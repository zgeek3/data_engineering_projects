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


staging_events_table_create = ("""CREATE TABLE IF NOT EXISTS staging_events (
	artist VARCHAR,
	auth VARCHAR,
	firstName VARCHAR,
	gender VARCHAR,
	ItemInSession INTEGER, 
	lastName VARCHAR,
	length DECIMAL,
	level VARCHAR, 
	location VARCHAR, 
	method VARCHAR, 
	page VARCHAR, 
	registration REAL,
	sessionId INTEGER,
	song VARCHAR,
	status VARCHAR,
	ts BIGINT,
	userAgent VARCHAR,
	userId INTEGER
	);
""")

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs (
	num_songs INTEGER,
	artist_id VARCHAR,
	artist_latitude DECIMAL,
	artist_longitude DECIMAL,
	artist_location VARCHAR,
	artist_name VARCHAR,
	song_id VARCHAR,
	title VARCHAR,
	duration DECIMAL,
	year INTEGER
	);
""")

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays (
	song_play_id INTEGER,
	start_time TIMESTAMP NOT NULL, 
	user_id INTEGER NOT NULL, 
	level VARCHAR(4), 
	song_id VARCHAR(50) NOT NULL, 
	artist_id VARCHAR(50) NOT NULL, 
	session_id INTEGER, 
	location VARCHAR(100), 
	user_agent VARCHAR(1500)
	);
""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS users (
	user_id INTEGER NOT NULL,
	first_name VARCHAR(50),
	last_name VARCHAR(50),
	gender VARCHAR(10),
	level VARCHAR(10), 
	PRIMARY KEY (user_id)
	);
""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs (
	song_id VARCHAR(50) NOT NULL, 
	title VARCHAR(500), 
	artist_id VARCHAR NOT NULL, 
	year INTEGER, 
	duration DECIMAL(10,5), 
	PRIMARY KEY (song_id)
	);
""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists (
	artist_id VARCHAR(50) NOT NULL, 
	name VARCHAR(500), 
	location VARCHAR(500), 
	latitude DECIMAL(15,6), 
	longitude DECIMAL(15,6), 
	PRIMARY KEY (artist_id)
	);
""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time (
	start_time TIMESTAMP  NOT NULL, 
	hour INTEGER, 
	day INTEGER, 
	week INTEGER, 
	month INTEGER, 
	year INTEGER, 
	weekday INTEGER, 
	PRIMARY KEY (start_time)
	);
""")

# STAGING TABLES



staging_events_copy = ("""
	copy staging_events
	from {}
	iam_role {}
	compupdate off 
	region 'us-west-2'
	JSON {};
""").format(config.get('S3','LOG_DATA'),config.get('IAM_ROLE','ARN'),config.get('S3','LOG_JSONPATH'))

staging_songs_copy = ("""
	copy staging_songs
	from {}
	iam_role {}
	compupdate off 
	region 'us-west-2'
	JSON 'auto' truncatecolumns;
""").format(config.get('S3','SONG_DATA'),config.get('IAM_ROLE','ARN'))

# FINAL TABLES

# Timestamp info here: https://stackoverflow.com/questions/39815425/how-to-convert-epoch-to-datetime-redshift

songplay_table_insert = ("""
	INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)  
	(SELECT
	TIMESTAMP 'epoch' + ev.ts/1000 * interval '1 second', 
	ev.userId, 
	ev.level, 
	so.song_id, 
	so.artist_id, 
	ev.sessionId, 
	ev.location, 
	ev.userAgent
	FROM staging_events ev 
	JOIN staging_songs so
	ON ev.song = so.title
	WHERE ev.page ='NextSong'
	);
""")

user_table_insert = ("""
	INSERT INTO users (user_id,first_name,last_name,gender,level)
	(SELECT
	DISTINCT ev.userId,
	ev.firstName,
	ev.lastName,
	ev.gender,
	ev.level
	FROM staging_events ev
	WHERE ev.page ='NextSong'
	AND ev.userId is NOT NULL
	);
""")

song_table_insert = ("""
	INSERT INTO songs (song_id, title, artist_id, year, duration)
	(SELECT
	DISTINCT so.song_id,
	so.title,
	so.artist_id,
	so.year,
	so.duration
	FROM staging_songs so
	);
""")

artist_table_insert = ("""
	INSERT INTO artists (artist_id, name, location, latitude, longitude)
	(SELECT
	DISTINCT so.artist_id,
	so.artist_name,
	so.artist_location,
	so.artist_latitude,
	so.artist_longitude
	FROM staging_songs so
	);
""")

time_table_insert = ("""
	INSERT INTO time (start_time,hour,day,week,month,year,weekday)
	(SELECT DISTINCT TIMESTAMP 'epoch' + ev.ts/1000 * interval '1 second',
	extract(hour from TIMESTAMP 'epoch' + ev.ts/1000 * interval '1 second'),
	extract(day from TIMESTAMP 'epoch' + ev.ts/1000 * interval '1 second'),
	extract(week from TIMESTAMP 'epoch' + ev.ts/1000 * interval '1 second'),
	extract(month from TIMESTAMP 'epoch' + ev.ts/1000 * interval '1 second'),
	extract(year from TIMESTAMP 'epoch' + ev.ts/1000 * interval '1 second'),
	extract(weekday from TIMESTAMP 'epoch' + ev.ts/1000 * interval '1 second')
	FROM staging_events ev
	WHERE ev.page ='NextSong'
	);
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
