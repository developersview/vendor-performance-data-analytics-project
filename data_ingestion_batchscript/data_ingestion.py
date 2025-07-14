# Use this script to save csv files into local PostgreSQL with filename as tablename
import pandas as pd
import os
from sqlalchemy import create_engine
import logging
import time

# Configure logging
logging.basicConfig(
    filename="logs/ingestion_db.log", 
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s", 
    filemode="a"
)

# PostgreSQL connection details (modify these as needed)
DB_USER = 'postgres'
DB_PASSWORD = '123456'  # replace with your actual password
DB_HOST = 'localhost'
DB_PORT = '5432'
DB_NAME = 'vendordb'  # replace with your actual database name

# SQLAlchemy connection string for PostgreSQL
engine = create_engine(
    f'postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
)

def ingest_db(df, table_name, engine):
    '''Ingest the dataframe into the database with given table name'''
    df.to_sql(table_name, con=engine, if_exists='replace', index=False)
    logging.info(f"Table '{table_name}' ingested successfully.")
    print(f"Table '{table_name}' ingested successfully.")

def load_raw_data():
    '''Load all CSVs from data/ and ingest into database'''
    start = time.time()
    for file in os.listdir('data'):
        if file.endswith('.csv'):
            file_path = os.path.join('data', file)
            df = pd.read_csv(file_path)
            logging.info(f'Ingesting {file} into database. Row Count: {len(df)}')
            print(f'Ingesting {file} into database. Row Count: {len(df)}')
            table_name = os.path.splitext(file)[0]
            ingest_db(df, table_name, engine)
    end = time.time()
    total_time = (end - start) / 60
    logging.info('--------------Ingestion Complete------------')
    logging.info(f'Total Time Taken: {total_time:.2f} minutes')
    print('--------------Ingestion Complete------------')
    print(f'Total Time Taken: {total_time:.2f} minutes')

if __name__ == '__main__':
    load_raw_data()
