import pandas as pd
import os
from sqlalchemy import create_engine
import logging
import time

# Configure logging
logging.basicConfig(
    filename="logs/vendor_summary_etl_pipeline.log", 
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s", 
    filemode="a"
)

# PostgreSQL connection details (modify these as needed)
DB_USER = 'postgres'
DB_PASSWORD = 'postgres'  # replace with your actual password
DB_HOST = 'localhost'
DB_PORT = '5432'
DB_NAME = 'vendordatabase'  # replace with your actual database name


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
                JOIN
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



if __name__ == '__main__':
    # SQLAlchemy connection string for PostgreSQL
    logging.info("Creating database engine...")
    engine = create_engine(
        f'postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
    )
    logging.info("Database engine created successfully.")

    # Start the ETL pipeline
    logging.info("Starting ETL pipeline for vendor summary data...")
    start_time = time.time()
    # Load the vendor summary data
    logging.info("Getting vendor summary data...")
    vendor_summary_df = get_vendor_summary(engine)
    logging.info("Vendor summary data retrieved successfully.")

    #clean the data
    logging.info("Cleaning vendor summary data...")
    tranformed_vendor_summary_df = tranform_vendor_summary_data(vendor_summary_df)
    logging.info("Vendor summary data cleaned successfully.")
    logging.info(f"Retrieved {len(tranformed_vendor_summary_df)} records from vendor summary data.")
    logging.info(f"Columns in transformed vendor summary data: \n{tranformed_vendor_summary_df.dtypes}")

    # Ingest the cleaned data into the database
    logging.info("Ingesting cleaned vendor summary data into the database...")
    ingest_db(tranformed_vendor_summary_df, 'vendor_sales_summary', engine)
    logging.info("Vendor summary data ingested successfully into 'vendor_sales_summary' table.")

    end_time = time.time()
    total_time = (end_time - start_time) / 60
    logging.info('--------------ETL Pipeline Complete------------')
    logging.info(f'Total Time Taken: {total_time:.2f} minutes')



