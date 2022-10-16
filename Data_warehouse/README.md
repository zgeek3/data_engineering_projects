# Udacity Data Warehouse Project - Z.M.

# Moving the Sparkify data to the cloud.

Sparkify is interested in songplay data, who listens to what, when, etc.  
They need complete data such as:
* Who is listening?
* Who are the artists?
* What are the songs?

Up until now they have benen storing their data in json files in s3 buckets and using
a postgres database, but now they want to move the data to REDSHIFT so that they will
have more flexibility in terms of servers, data size, accessibility, etc.

There are thre major steps in the ETL pipelline

1. Creating the necessary tables.
2. Uploading the data from S3 to staging tables.
3. Copying the key data from the staging tables into a a single fact table and 2
Dimension Tables.

## The Tables

### Staging Tables

The staging tables will be used to copy the data from the s3 buckets into 
the Reshift.

#### staging_events

The staging_events data will come from the log data data found here:
Log data: s3://udacity-dend/log_data
Log data json path:  s3://udacity-dend/log_json_path.json

The data will be imported using the copy compand and wil be entered in the staging_events
table with the following columns

staging_event_id INTEGER IDENTITY(0,1) NOT NULL PRIMARY KEY
artist VARCHAR(150)
auth VARCHAR(50)
firstName VARCHAR(50)
gender VARCHAR(10)
ItemInSession INTEGER
lastName VARCHAR(50)
length DECIMAL (10,5)
level VARCHAR(10)
location VARCHAR(100)
method VARCHAR(10)
page VARCHAR(20)
registration REAL
sessionId INTEGER
song VARCHAR(250)
status VARCHAR(50)
ts BIGINT
userAgent VARCHAR(1500)
userId INTEGER

#### staging_songs

The staging_songs data will come from the log data data found here:
Log data: s3://udacity-dend/song_data

The data will be imported using the copy compand and wil be entered in the song_events
table with the following columns

num_songs INTEGER
artist_id VARCHAR(50)
artist_latitude DECIMAL(15,6)
artist_longitude DECIMAL(15,6)
artist_location VARCHAR(100)
artist_name VARCHAR(150)
song_id VARCHAR(50)
title VARCHAR(50)
duration DECIMAL(10,5)
year INTEGER

### Tables used for analytics

Extracting data from the staging tables the tables for analytics will be created


#### Fact Table

songplays - records in event data associated with song plays i.e. records with page NextSong
songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

#### Dimension Tables

* users - users in the app
** user_id, first_name, last_name, gender, level

* songs - songs in music database
** song_id, title, artist_id, year, duration

* artists - artists in music database
** artist_id, name, location, lattitude, longitude

* time - timestamps of records in songplays broken down into specific units
** start_time, hour, day, week, month, year, weekday

## The pipeline

There are four files necessary to run the pipeline

1.  dwh.cfg
This file contains the following settings that will be used to access
the aws reshift cluser.  The user is expected to provide their own cluster
and fill in the necessary information.

[CLUSTER]
HOST={insert_info here}
DB_NAME={insert_info here}	
DB_USER={insert_info here}
DB_PASSWORD={insert_info here}
DB_PORT={insert_info here}

[IAM_ROLE]
ARN='{insert_info here}'

[S3]
LOG_DATA='s3://udacity-dend/log-data'
LOG_JSONPATH='s3://udacity-dend/log_json_path.json'
SONG_DATA='s3://udacity-dend/song-data'

2.  sql_queries.py

This file contains all the necessary support queries to create the tables and insert the data.

3.  create_tables.py

create_tables.py will drop existing tables with the same name as those listed above and
create new tables.

4.  etl.py

* Step 1.  The etl will import the data from S3 into the staging tables.
* Step 2.  The etl will copy the imported data to the desired fact and dimension tables

## How to use this pipeline

1.  Create a redshift cluster & iam_role
2.  Enter in the information into the dwh.cfg
3.  Run 'python create_tables.py' -- This will take just a few moments to finish.
4.  Run 'python etl.py' -- due to the large data size, this may take an hour or more
to complete.

4










