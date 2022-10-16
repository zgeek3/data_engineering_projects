# Sparkify ETL README

Sparkify is interested in songplay data, who listens to what, when, etc.  
They need complete data such as:
* Who is listening?
* Who are the artists?
* What are the songs?

But they are going to do a lot of analysis on this so they will want 
information about songplays in one table to improve the response rate 
of basic analytics.  So setting a Star Schema with a fact table and 
several dimension tables for ease of use.

## The Sparkify ETL pipeline will do the following:

## Read song json files and create:

### 1. SONGS table with the following columns: (Dimension Table)

'song_id','title','artist_id', 'year', 'duration'

### 2. ARTISTS table with the following columns: (Dimension Table)

'artist_id','artist_name','artist_location','artist_latitude','artist_longitude'

Read in log data from from json files and create:

### 3. Create the TIME table with the folowing columns: (Dimension Table)

'start_time','hour',day','week','month','year','weekday'

The 'start_time' will be based on the ts time in ms in the original data 
converted to a timestamp.
'hour',day','week','month','year', and 'weekday' will be dervied from this timestamp.

### 4. Create the USERS table with the following columns: (Dimension Table)

'userId', 'firstName', 'lastName', 'gender', 'level'

### 5. Create the SONGPLAYS table with the folowing columns: (Fact Table)

'start_time', 'user_id', 'level', 'song_id', 'artist_id', 'session_id', 
'location', 'user_agent'

This table contains information combined from the song data and the 
log data and can be used for doing a lot of analytical research.

Example query to find the average number of song plays per user in the database:

select round(avg(t.c),2) as average_song_plays from 
(select count(*) as c from songplays s group by s.user_id) t


## To run the ETL:

### Expected:

A data directory in the same directory as the python files with two directories:
1. song_data - contains json data of song data
2. log_data - contains json data of song plays

The following three files 

* create_tables.py - Run this first to setup the appropriate database tables
* etl.py - Run this to upload the convert the json data into tables in thed database
* sql_queries.py - This is a reference file that will not be run by itself









