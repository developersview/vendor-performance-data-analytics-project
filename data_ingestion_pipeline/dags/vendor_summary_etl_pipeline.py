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

def get_vendor_summary(engine):
    """Fetch vendor sales summary from the database and return as a DataFrame."""
    vendor_sales_summary = pd.read_sql_query(
        """
        WITH FrieghtSummary AS (
            SELECT 
                vendornumber,
                SUM(freight) AS freightcost
            FROM
                vendor_invoice
            GROUP BY vendornumber
            ORDER BY vendornumber
        ),
        PurchaseSummary AS (
            SELECT
                p.vendornumber,
                p.vendorname,
                p.brand,
                p.purchaseprice,
                pp.description,
                pp.volume,
                pp.price AS actual_price,
                SUM(p.quantity) AS total_purchase_quantity,
                SUM(p.dollars) AS total_purchase_dollars
            FROM
                purchases p
            JOIN
                purchase_prices pp ON p.brand = pp.brand
            GROUP BY p.vendornumber, p.vendorname, p.brand, p.PurchasePrice, pp.Price, pp.Volume, pp.description
            HAVING SUM(p.dollars) > 0
            ORDER BY total_purchase_dollars
        ),
        SalesSummary AS (
            SELECT
                vendorno,
                brand,
                SUM(salesdollars) AS totalsalesdollars,
                SUM(salesprice) AS totalsalesprice,
                SUM(salesquantity) AS totalsalesquantity,
                SUM(excisetax) AS totalexcisetax
            FROM
                sales
            GROUP BY 1, 2
        )
        SELECT
            ps.vendornumber,
            ps.vendorname,
            ps.description,
            ps.brand,
            ps.purchaseprice,
            ps.volume,
            ps.actual_price,
            ps.total_purchase_quantity,
            ps.total_purchase_dollars,
            ss.totalsalesdollars,
            ss.totalsalesprice,
            ss.totalsalesquantity,
            ss.totalexcisetax,
            fs.freightcost
            
        FROM
            PurchaseSummary ps
                LEFT JOIN
            SalesSummary ss ON ps.vendornumber = ss.vendorno AND ps.brand = ss.brand
                LEFT JOIN
            FrieghtSummary fs ON ps.vendornumber = fs.vendornumber
        ORDER BY ps.total_purchase_dollars DESC;       
        """, engine
    )

    return vendor_sales_summary



def tranform_vendor_summary_data(vendor_summary_df):
    """Clean the vendor summary data and calculate additional metrics."""

    vendor_summary_df.fillna(0, inplace=True)
    vendor_summary_df['volume'] = vendor_summary_df['volume'].astype('float')
    vendor_summary_df['vendorname'] = vendor_summary_df['vendorname'].str.strip()

    vendor_summary_df['grossprofit'] = vendor_summary_df['totalsalesdollars'] - vendor_summary_df['total_purchase_dollars']
    vendor_summary_df['profitmargin'] = (vendor_summary_df['grossprofit'] / vendor_summary_df['totalsalesdollars']) * 100
    vendor_summary_df['stockturnover'] = vendor_summary_df['totalsalesquantity'] / vendor_summary_df['total_purchase_quantity']
    vendor_summary_df['salestopurchaseratio'] = vendor_summary_df['totalsalesdollars'] / vendor_summary_df['total_purchase_dollars']

    return vendor_summary_df



def ingest_db(df, table_name, engine):
    """Ingest the dataframe into the database with given table name"""
    df.to_sql(table_name, con=engine, if_exists='replace', index=False)


with DAG(
    dag_id='vendor_summary_etl_pipeline',
    default_args=default_args,
    schedule='@daily',  # Set to None for manual trigger
    start_date=pendulum.datetime(2025, 7, 9, tz="UTC"),
    catchup=False,
    tags=['vendor', 'summary', 'etl'],
) as dag:

    @task
    def extract_vendor_summary():
        """Extract vendor sales summary from the database."""
        start_time = time.time()
        logging.info("Starting extraction of vendor sales summary data.")
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        engine = pg_hook.get_sqlalchemy_engine()

        vendor_summary_df = get_vendor_summary(engine)
        path = os.path.join(DATA_FILE_PATH, 'vendor_summary_raw.csv')
        vendor_summary_df.to_csv(path, index=False)

        end_time = time.time()
        logging.info(f"Extraction completed in {end_time - start_time:.2f} seconds.")
        logging.info(f"Extracted {len(vendor_summary_df)} records from vendor sales summary.")
        return path


    @task
    def transform_vendor_summary(file_path:str):
        """Transform the vendor summary data."""
        start_time = time.time()
        logging.info("Starting transformation of vendor summary data.")

        vendor_summary_df = pd.read_csv(file_path)
        tranformed_vendor_summary_df = tranform_vendor_summary_data(vendor_summary_df)
        path = os.path.join(DATA_FILE_PATH, 'vendor_summary_transformed.csv')
        tranformed_vendor_summary_df.to_csv(path, index=False)

        end_time = time.time()
        logging.info(f"Transformation completed in {end_time - start_time:.2f} seconds.")
        logging.info(f"Columns in transformed vendor summary data: \n{tranformed_vendor_summary_df.dtypes}")
        return path
    

    @task
    def load_vendor_summary(file_path: str):
        """Load the transformed vendor summary data into the database."""
        start_time = time.time()
        logging.info("Starting loading of vendor summary data into the database.")
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        engine = pg_hook.get_sqlalchemy_engine()

        vendor_summary_df = pd.read_csv(file_path)
        ingest_db(vendor_summary_df, 'vendor_sales_summary', engine)

        end_time = time.time()
        logging.info(f"Loading completed in {end_time - start_time:.2f} seconds.")
        logging.info("Vendor summary data loaded successfully into 'vendor_sales_summary' table.")



    # Define the ETL pipeline tasks
    vendor_csv = extract_vendor_summary()
    transformed_csv = transform_vendor_summary(vendor_csv)
    load_vendor_summary(transformed_csv)