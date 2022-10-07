from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (
    StageToRedshiftOperator,
    LoadFactOperator,
    LoadDimensionOperator,
    DataQualityOperator,
)
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

LOG_DATA = "s3://udacity-dend/log_data/2018/11"
SONG_DATA = "s3://udacity-dend/song_data/A/A/A"
LOG_JSONPATH = "s3://udacity-dend/log_json_path.json"

default_args = {
    "owner": "pedro-tofani",
    "start_date": datetime(2021, 8, 1),
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry": False,
    "catchup": False,
}

dag = DAG(
    "udac_example_dag",
    default_args=default_args,
    description="Load and transform data in Redshift with Airflow",
    schedule_interval='0 * * * *'
)

start_operator = DummyOperator(task_id="Begin_execution", dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id="Stage_events",
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_events",
    s3_path=LOG_DATA,
    log_json_file=LOG_JSONPATH,
    provide_context=True,
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id="Stage_songs",
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_songs",
    s3_path=SONG_DATA,
    provide_context=True,
)

load_songplays_table = LoadFactOperator(
    task_id="Load_songplays_fact_table",
    dag=dag,
    redshift_conn_id="redshift",
    table="songplays",
    sql_query=SqlQueries.songplay_table_insert,
)

load_user_dimension_table = LoadDimensionOperator(
    task_id="Load_user_dim_table",
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    sql_query=SqlQueries.user_table_insert,
    table="users",
)

load_song_dimension_table = LoadDimensionOperator(
    task_id="Load_song_dim_table",
    dag=dag,
    redshift_conn_id="redshift",
    sql_query=SqlQueries.song_table_insert,
    table="songs",
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id="Load_artist_dim_table",
    dag=dag,
    redshift_conn_id="redshift",
    sql_query=SqlQueries.artist_table_insert,
    table="artists",
)

load_time_dimension_table = LoadDimensionOperator(
    task_id="Load_time_dim_table",
    dag=dag,
    redshift_conn_id="redshift",
    sql_query=SqlQueries.time_table_insert,
    table="time",
)

run_quality_checks = DataQualityOperator(
    task_id="Run_data_quality_checks",
    dag=dag,
    redshift_conn_id="redshift",
    tables=["artists", "songplays", "songs", "time", "users"],
)

end_operator = DummyOperator(task_id="Stop_execution", dag=dag)

start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift

stage_songs_to_redshift >> load_songplays_table
stage_events_to_redshift >> load_songplays_table

(
    load_songplays_table
    >> [
        load_user_dimension_table,
        load_song_dimension_table,
        load_artist_dimension_table,
        load_time_dimension_table,
    ]
    >> run_quality_checks
    >> end_operator
)
