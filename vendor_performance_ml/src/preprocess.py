import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler, MinMaxScaler
import joblib
import logging

#set up logging
logging.basicConfig(
    filename='../logs/ml_pipeline.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

RAW_DATA_PATH = '../data/final_vendor_analysis.csv'
PROCESSED_DATA_PATH = '../data/processed_data.csv' 
SCALER_PATH = '../models/scaler.pkl'

DROP_COLUMNS = [
    'vendornumber', 'vendorname', 'description', 'grossprofit', 'profitmargin', 'ordersize'
]

def preprocess_data():
    """
    Preprocess the raw vendor performance data.
    """
    try:
        # Load raw data
        logging.info("Loading data from %s", RAW_DATA_PATH)
        df = pd.read_csv(RAW_DATA_PATH)
        logging.info("Data loaded successfully with shape: %s", df.shape)
        logging.info(f"Columns in raw data: {df.columns.tolist()}")
        logging.info(f"Data Information: {df.dtypes}")

        # Drop unnecessary columns
        logging.info("Dropping unnecessary columns: %s", DROP_COLUMNS)
        df.drop(columns=DROP_COLUMNS, inplace=True, errors='ignore')
        logging.info(f"Data Information after dropping columns: {df.dtypes}")

        # Handle missing values
        if df.isnull().values.any():
            logging.info("Handling missing values with mean if applicable")
            df.fillna(df.mean(), inplace=True)

        # Feature scaling
        logging.info("Scaling features using StandardScaler")
        scaler = StandardScaler()
        scaled_features = scaler.fit_transform(df)
        df_scaled = pd.DataFrame(scaled_features, columns=df.columns)
        
        #save the processed data
        logging.info("Saving processed data to %s", PROCESSED_DATA_PATH)
        df_scaled.to_csv(PROCESSED_DATA_PATH, index=False)
        joblib.dump(scaler, SCALER_PATH)
        logging.info("Scaler saved to %s", SCALER_PATH)
        logging.info("Data preprocessing completed successfully")

    except Exception as e:
        logging.error("Error in preprocessing data: %s", e)


if __name__ == "__main__":
    logging.info("---------------Starting data preprocessing----------------")
    preprocess_data()
    logging.info("---------------Data preprocessing completed----------------")