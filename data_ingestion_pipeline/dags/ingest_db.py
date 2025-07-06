#import neccessary libraries
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task
import pendulum
import logging
import os
import time
import pandas as pd

DATA_FILE_PATH = './data'
POSTGRES_CONN_ID = 'postgres_default'
CHUNK_SIZE = 50000

logging.basicConfig(
    filename="logs/ingestion_db.log", 
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s", 
    filemode="a"  
)

default_args = {
    'owner': 'airflow',
    'start_date': pendulum.datetime(2025, 7, 6, tz="UTC"),
}

# Define the DAG
with DAG(
    dag_id='database_ingestion_pipeline',
    default_args=default_args,
    schedule='@daily',  # Set to None for manual triggering
    catchup=False,
    tags=['ingest', 'db'],
) as dag:
    
    @task
    def load_raw_data():
        """
        load raw data at 1st step
        """
        logging.info(f"Data file path: {DATA_FILE_PATH}")
        start_time = time.time()
        data_folder = DATA_FILE_PATH
        logging.info(f"Data folder: {data_folder}")
        csv_data = []

        for file in os.listdir(data_folder):
            if file.endswith('.csv'):
                file_path = os.path.join(data_folder, file)
                logging.info(f"Loading data from {file_path}")
                df = pd.read_csv(file_path) 
                csv_data.append(file_path)
                logging.info(f"Completed reading {file}")

        logging.info(f"Total files read: {len(csv_data)}")
        end_time = time.time()
        time_taken_sec = end_time - start_time
        time_taken_min = time_taken_sec / 60
        logging.info(f"Time taken to load data: {time_taken_sec} seconds")
        logging.info(f"Time taken to load data: {time_taken_min:.2f} mianutes")
        return csv_data
    
    @task
    def ingest_data_to_db(file_paths: list):    
        """ 
        Ingest data into the database
        """
        start_time = time.time()
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        engine = pg_hook.get_sqlalchemy_engine()

        for path in file_paths:
            filename = os.path.basename(path)
            table_name = os.path.splitext(filename)[0]
            logging.info(f"Reading {filename} for ingestion")

            try:
                for i, chunk in enumerate(pd.read_csv(path, chunksize=CHUNK_SIZE)):
                    mode = 'replace' if i == 0 else 'append'
                    chunk.to_sql(table_name, con=engine, if_exists=mode, index=False)
                    logging.info(f"✅ Chunk {i + 1} of {filename} ingested")
                logging.info(f"🎯 {filename} fully ingested into {table_name}")
            except Exception as e:
                logging.error(f"❌ Failed to ingest {filename}: {e}")

        end_time = time.time()
        time_taken_sec = end_time - start_time
        time_taken_min = time_taken_sec / 60
        logging.info(f"Time taken to load data: {time_taken_sec} seconds")
        logging.info(f"Time taken to load data: {time_taken_min:.2f} minutes")

    
    # Define the task dependencies
    csv_file_paths = load_raw_data()
    ingest_data_to_db(csv_file_paths)