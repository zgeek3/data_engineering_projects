Requirement:
Will have to run the create tables sql before starting the dag. Better to use the 
one from postgres project since it drops existing tables first which the code provided
does not.

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

They will need the data updated frequently so that their analysis will 
b reflective of the latest data. They have agreed to the data being at 
most 1 hour old.

## The Sparkify ETL pipeline will do the following:

## Read song and log json files from s3 every hour and update the following tables:

### 1. SONGS table with the following columns: (Dimension Table)

'song_id','title','artist_id', 'year', 'duration'

### 2. ARTISTS: (Dimension Table)

'artist_id','artist_name','artist_location','artist_latitude','artist_longitude'

### 3. TIME: (Dimension Table)

'start_time','hour',day','week','month','year','weekday'

### 4. USERS (Dimension Table)

'userId', 'firstName', 'lastName', 'gender', 'level'

### 5. SONGPLAYS (Fact Table)

'start_time', 'user_id', 'level', 'song_id', 'artist_id', 'session_id', 
'location', 'user_agent'

## To run the ETL:
1. <project_directory>/create_tables.sql in the database to create all the necessary tables with
expected columns.
2. Upload the files contained in this project to the appropriate airflow 
directory.
3.  In Airflow configture 
* 'aws_credentials' under connections
* PostSQL connection to the redshift database under connections Note: The database
will need to be setup to allow external connections.
4. Turn on the Airflow dag

### Expected:

Every hour the dag will run and delete the old data and import the latest data and run a quality check.
