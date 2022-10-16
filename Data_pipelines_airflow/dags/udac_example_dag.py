from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

# To start airflow in udacity workspace: /opt/airflow/start.sh

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')



default_args = {
    'owner': 'zm',
    'start_date': datetime(2020, 1, 5),
    'email_on_retry': False,
    'retries': 3,
    'catchup': False,
    'retry_delay': timedelta(minutes=5),
    'depends_on_past': False,
   
}

#
# The following DAG performs the following functions:
# Example -- needs to be updated
#
#       1. Loads events and songs from S3 to staging tables
#       2. Creates and load the songplays fact table to redshift
#       3. Creates and loadsthe related dimension tables: song, user, artist, time to redshift
#       4. Runs data quality checks on the created tables
#

dag = DAG('data_pipelines_project',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *',
          max_active_runs=1
        )
#
# Start of the dag
#

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

#
# Loading events data from s3 to redshift
#

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    table="staging_events",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket='udacity-dend',
    s3_key='log_data',
    s3_region='us-west-2',
    format_info="format as json 's3://udacity-dend/log_json_path.json'",
    dag=dag
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    table="staging_songs",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket='udacity-dend',
    s3_key='song_data/A/A/A',
    s3_region='us-west-2',
    format_info="format as json 'auto'",
    dag=dag
)

#
# Loading the desired tables from the staging table data
#

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    table='songplays',
    redshift_conn_id="redshift",
    sql_to_run=SqlQueries.songplay_table_insert,
    dag=dag
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    table='users',
    redshift_conn_id="redshift",
    sql_to_run=SqlQueries.user_table_insert,
    append_status=False,
    dag=dag
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    table='songs',
    redshift_conn_id="redshift",
    sql_to_run=SqlQueries.song_table_insert,
    append_status=False,
    dag=dag
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    table='artists',
    redshift_conn_id="redshift",
    sql_to_run=SqlQueries.artist_table_insert,
    append_status=False,
    dag=dag
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    table='time',
    redshift_conn_id="redshift",
    sql_to_run=SqlQueries.time_table_insert,
    append_status=True,
    dag=dag
)

#
# Doing a quick data check on the data that was created.
#

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    redshift_conn_id="redshift",
    tables=["songplays", "users", "songs", "artists", "time"],
    dag=dag
)

#
# End of the dag
#
end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)


#
# Task ordering for the DAG tasks 
#

start_operator >> [stage_events_to_redshift,stage_songs_to_redshift]
[stage_events_to_redshift,stage_songs_to_redshift] >> load_songplays_table

load_songplays_table >> [load_song_dimension_table,load_user_dimension_table,load_artist_dimension_table, load_time_dimension_table]

[load_song_dimension_table,load_user_dimension_table,load_artist_dimension_table, load_time_dimension_table] >> run_quality_checks

run_quality_checks  >> end_operator
