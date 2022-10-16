# Setting up the imports that will be needed for the run

import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofweek
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, LongType, TimestampType
from pyspark.sql.functions import monotonically_increasing_id 


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    This function will setup the spark session.

    Parameters:
        None

    Returns:
        None
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    This function will: 
     * Load song data from the S3 input.
     * Extract data for the songs table and write to parquet files on S3.
     * Extract data for the artists table and write to parquet files on S3.
    

    Parameters:
        spark: spark connection
        input_data: S3 path to the song parquet files
        output_data: S3 path where the parquet files will be saved

    Returns:
        None
    """

    # get filepath to song data file
    song_data = input_data
    
    # read song data file
    df = spark.read.json(song_data)  

    # extract columns to create songs table
    songs_table = df.select("song_id", "title", "artist_id", "year", "duration").dropDuplicates()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.parquet(path=os.path.join(output_data,'songs.parquet'),mode='overwrite',partitionBy = \
        ('year','artist_id'))

    # extract columns to create artists table
    artists_table = df.selectExpr("artist_id", "artist_name as name",
        "artist_location as location","artist_latitude as latitude", 
        "artist_longitude as longitude").dropDuplicates() 
    
    # write artists table to parquet files
    artists_table.write.parquet(path=os.path.join(output_data,'artists.parquet'),mode='overwrite')


def process_log_data(spark, input_data, output_data):
    """
    This function will: 
     * Load log data from the S3 input.
     * Extract data for the users table and write to parquet files on S3.
     * Extract data for the timestamp table and write to parquet files on S3.
     * Combine tables to create a songplays table and write to parquet files on S3.
    

    Parameters:
        spark: spark connection
        input_data: S3 path to the log parquet files
        output_data: S3 path where the parquet files will be saved

    Returns:
        None
    """

    # get filepath to log data file
    log_data = input_data

    # read log data file
    df = spark\
        .read\
        .option('inferSchema', True)\
        .json(log_data)\
        .cache() 
    
    # filter by actions for song plays
    df = df.filter(df.page =="NextSong")

    # extract columns for users table    
    users_table = df.selectExpr("userId as user_id", "firstName as first_name", "lastName as last_name", \
        "gender", "level").dropDuplicates() 
    
    # write users table to parquet files
    users_table.write.parquet(path=os.path.join(output_data,'users.parquet'),mode='overwrite')
    
    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: int(int(x)/1000), IntegerType())
    df = df.withColumn('new_ts', get_timestamp(df.ts))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: datetime.fromtimestamp((x/1000.0)), TimestampType()) 
    df =  df.withColumn('timestamp', get_datetime(df.ts))
    
    df.show(5)

    df = df.withColumn('hour',hour(df.timestamp))
    df = df.withColumn('day',dayofmonth(df.timestamp))
    df = df.withColumn('week',weekofyear(df.timestamp))
    df = df.withColumn('month',month(df.timestamp))
    df = df.withColumn('year',year(df.timestamp))
    df = df.withColumn('weekday',dayofweek(df.timestamp))
    
    # extract columns to create time table
    time_table = df.selectExpr("ts as start_time", "hour", "day", \
        "week", "month","year","weekday").dropDuplicates()
    
    # write time table to parquet files partitioned by year and month
    time_table.write.parquet(path=os.path.join(output_data,'time.parquet'),mode='overwrite', \
        partitionBy = ('year','month'))

    # read in song data to use for songplays table
    song_df = spark.read.parquet(os.path.join(output_data,'songs.parquet'))

    # read in song data to use for artists table
    artists_df = spark.read.parquet(os.path.join(output_data,'artists.parquet'))

    # read in song data to use for artists table
    time_df = spark.read.parquet(os.path.join(output_data,'time.parquet'))

    # extract columns from joined song and log datasets to create songplays table 

    # Joining the artist_id (from the artists table) to the log file to create songplay dataframe
    songplay_table = df.join(artists_df, [df.artist == artists_df.name]).select(df.ts, df.userId, \
        df.song,artists_df.artist_id,df.level,df.sessionId,df.location,df.userAgent,df.song)

    # Joining the song_id (from the songs table) to the songplay dataframe
    songplay_table = songplay_table.join(song_df,[songplay_table.artist_id == song_df.artist_id, \
        songplay_table.song == song_df.title]).select(songplay_table.ts, songplay_table.userId, \
        songplay_table.level,song_df.song_id,songplay_table.artist_id,songplay_table.sessionId, \
        songplay_table.location,songplay_table.userAgent)

    # Joining the year and month (from the time table) to the songplay dataframe (used for partitioning)
    songplay_table = songplay_table.join(time_df,[songplay_table.ts == time_df.start_time]). \
    select(songplay_table.ts, songplay_table.userId, songplay_table.level,songplay_table.song_id, \
        songplay_table.artist_id,songplay_table.sessionId,songplay_table.location, \
        songplay_table.userAgent,time_df.year,time_df.month)

    # Renaming the songplay columns to the desired text
    songplay_table = songplay_table.selectExpr("ts as start_time","userId as user_id","song_id", \
        "artist_id","sessionId as session_id","location","userAgent as user_agent","year","month")
 
    # Creating the songplays songplay_id

    songplay_table = songplay_table.select("*").withColumn("songplay_id", \
        monotonically_increasing_id()).selectExpr("songplay_id","start_time","user_id","song_id", \
        "artist_id","session_id","location","user_agent","year","month")

    # write songplays table to parquet files partitioned by year and month
    songplay_table.write.parquet(path=os.path.join(output_data,'songplays.parquet'),\
        mode='overwrite',partitionBy = ("year","month"))


def main():
    """
    This function will fun the pipeline

    Parameters:
        None

    Returns:
        None
    """
    spark = create_spark_session()
    input_song_data = "s3a://udacity-dend/song_data/*/*/*/*.json"
    input_log_data = "s3a://udacity-dend/log_data/*/*/*.json"
    output_data = "s3a://<>" # replace with your own storage bucket
    
    process_song_data(spark, input_song_data, output_data)    
    process_log_data(spark, input_log_data, output_data)


if __name__ == "__main__":
    main()
