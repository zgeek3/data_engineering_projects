# Udacity Data Lake Project - Z.M.

# Moving the Sparkify data to the cloud.

Sparkify is interested in songplay data, who listens to what, when, etc.  
They need complete data such as:
* Who is listening?
* Who are the artists?
* What are the songs?

Up until now they have benen storing their data in json files in s3 buckets and using
a postgres database, but now they want to move the data to parquet so that they will
have more flexibility in terms of how they use the data

There are thre major steps in the ETL pipelline

1. Loading the data from S3
2. Creating the necessary tables.
3. Uploading the new tables to S3 to parquet files.

Input files:
    input_song_data = "s3://udacity-dend/song_data/*/*/*/*.json"
    input_log_data = "s3://udacity-dend/log_data/*/*/*.json"


#### Fact Table

songplays - records in event data associated with song plays i.e. records with page NextSong
songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent, year, month
	- Partitioned in S3 by year and month

#### Dimension Tables

* users - users in the app
** user_id, first_name, last_name, gender, level

* songs - songs in music database
** song_id, title, artist_id, year, duration

* artists - artists in music database
** artist_id, name, location, lattitude, longitude
	- Partitioned in S3 by year and artist_id

* time - timestamps of records in songplays broken down into specific units
** start_time, hour, day, week, month, year, weekday

## The pipeline

There are four files necessary to run the pipeline

1.  dwh.cfg
This file contains the following settings that will be used to access
the aws s3 bucket.  The user is expected to provide their own bucket and fill 
in the necessary information

AWS_ACCESS_KEY_ID={insert value here}
AWS_SECRET_ACCESS_KEY={Insert value here}

2.  etl.py

This file loads all the data from S3 and creates the new data in S3

3.  Optional: Sparkify_notebook.ipynb

This is a notebook that can be run in a EMR spark cluster to achieve the same results


## How to use this pipeline

Run 'python etl.py' on a spark server -- This will take different amounts of time
based on how the server is setup.












