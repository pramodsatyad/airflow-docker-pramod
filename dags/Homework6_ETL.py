from airflow.decorators import task
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import get_current_context
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from datetime import datetime
from datetime import timedelta
import logging
import snowflake.connector

def return_snowflake_conn():

    # Initialize the SnowflakeHook
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')

    # Execute the query and fetch results
    conn = hook.get_conn()
    return conn.cursor()


@task
def create_table_load(cursor):

    try:

        cursor.execute("BEGIN")
        cursor.execute('''CREATE TABLE IF NOT EXISTS raw.user_session_channel (
                            userId int not NULL,
                            sessionId varchar(32) primary key,
                            channel varchar(32) default 'direct'  
                            );
                        ''')
        cursor.execute('''CREATE TABLE IF NOT EXISTS raw.session_timestamp (
                            sessionId varchar(32) primary key,
                            ts timestamp  
                            );
                        ''')
        # for the following query to run, 
        # the S3 bucket should have LIST/READ privileges for everyone

        #Create stage to prepare to load data
        cursor.execute('''CREATE OR REPLACE STAGE raw.blob_stage
                            url = 's3://s3-geospatial/readonly/'
                            file_format = (type = csv, skip_header = 1, field_optionally_enclosed_by = '"');
                        ''')
        
        #loading
        cursor.execute('''COPY INTO raw.user_session_channel
                          FROM @raw.blob_stage/user_session_channel.csv; ''')
        cursor.execute('''COPY INTO raw.session_timestamp
                          FROM @raw.blob_stage/session_timestamp.csv; ''')
        
        cursor.execute("COMMIT;")
        
        print("Tables created successfully")
        print("Data loaded successfully")

    except Exception as e:
        cursor.execute("ROLLBACK;")
        print(e)
        raise e 


with DAG(
    dag_id = 'ELT_DAG_Pramod',
    start_date = datetime(2024,10,27),
    catchup=False,
    tags=['ELT'],
    schedule = '45 2 * * *'
) as dag:
    cursor = return_snowflake_conn()
    create_table_load(cursor)