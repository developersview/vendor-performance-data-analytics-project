import pandas as pd
import joblib
import logging
from sklearn.linear_model import LinearRegression
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.ensemble import RandomForestRegressor

# Set up logging
logging.basicConfig(
    filename='../logs/ml_pipeline.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

RAW_DATA_PATH = '../data/final_vendor_analysis.csv'
PROCESSED_DATA_PATH = '../data/processed_data.csv'
MODEL_PATH = '../models/regression_model.pkl'

def train_regression_model():
    """
    Train a regression model on the preprocessed vendor performance data.
    """
    logging.info('Loading preprocessed features and target variable...')
    x = pd.read_csv(PROCESSED_DATA_PATH)
    y = pd.read_csv(RAW_DATA_PATH)['profitmargin']

    # Split the data into training and testing sets
    logging.info('Splitting data into training and testing sets...')
    X_train, X_test, y_train, y_test = train_test_split(x, y, test_size=0.2, random_state=42)

    # Initialize and train the regression model
    logging.info('Training the regression model...')
    #model = LinearRegression()
    # Alternatively, you can use RandomForestRegressor
    model = RandomForestRegressor(n_estimators=100, random_state=42)
    model.fit(X_train, y_train)

    # Prediction and evaluation
    logging.info('Making predictions on the test set...')
    y_pred = model.predict(X_test)

    r2_score_value = r2_score(y_test, y_pred)
    mae = mean_absolute_error(y_test, y_pred)
    mse = mean_squared_error(y_test, y_pred)

    logging.info("Model evaluation metrics:")
    logging.info(f"R^2 Score: {r2_score_value}")
    logging.info(f"Mean Absolute error: {mae}")
    logging.info(f"Mean Squared error: {mse}")

    # Save the trained model
    logging.info('Saving the trained model...')
    joblib.dump(model, MODEL_PATH)
    logging.info(f'Model saved to {MODEL_PATH}')


if __name__ == "__main__":
    logging.info("---------------Starting regression model training----------------")
    train_regression_model()
    logging.info("---------------Regression model training completed----------------")  
