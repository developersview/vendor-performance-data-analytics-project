import pandas as pd
import joblib
import logging
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
from matplotlib import pyplot as plt

# Set up logging
logging.basicConfig(
    filename='../logs/ml_pipeline.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

RAW_DATA_PATH = '../data/final_vendor_analysis.csv'
PROCESSED_DATA_PATH = '../data/processed_data.csv'
MODEL_PATH = '../models/clustering_model.pkl'
CLUSTER_OUTPUT_PATH = '../data/clustering_output.csv'

def train_clustering_model(n_clusters=3):
    """
    Train a clustering model on the preprocessed vendor performance data.
    """
    logging.info('Loading preprocessed data...')
    df = pd.read_csv(PROCESSED_DATA_PATH)

    # Feature scaling
    logging.info('Scaling features using StandardScaler...')
    scaler = StandardScaler()
    scaled_features = scaler.fit_transform(df)
    
    # KMeans clustering
    logging.info(f'Training KMeans clustering model with {n_clusters} clusters...')
    kmeans = KMeans(n_clusters=n_clusters, random_state=42, n_init=10)
    kmeans.fit(scaled_features)

    #assigning cluster labels to the original dataframe
    cluster_labels = kmeans.labels_
    df_clusterd = df.copy()
    df_clusterd['Cluster'] = cluster_labels

    # Save the trained model
    logging.info('Saving the trained clustering model...')
    joblib.dump(kmeans, MODEL_PATH)
    logging.info(f'Model saved to {MODEL_PATH}')

    # Save clustering output
    logging.info(f'Saving clustering output to {CLUSTER_OUTPUT_PATH}')
    df_clusterd.to_csv(CLUSTER_OUTPUT_PATH, index=False)
    logging.info(f'Clustering output saved to {CLUSTER_OUTPUT_PATH}')


    #plotting cluster
    plt.figure(figsize=(10, 6))
    plt.scatter(df_clusterd.iloc[:, 0], df_clusterd.iloc[:, 1], c=cluster_labels, cmap='viridis', marker='o')
    plt.title('Clusters of Vendors')
    plt.xlabel('Feature 1')
    plt.ylabel('Feature 2')
    plt.colorbar(label='Cluster Label')
    plt.savefig('../plots/clusters_plot.png')

    sse = []
    for k in range(1, 10):
        km = KMeans(n_clusters=k, random_state=42, n_init=10)
        km.fit(scaled_features)
        sse.append(km.inertia_)

    plt.plot(range(1, 10), sse, marker='o')
    plt.title('Elbow Method - Optimal k')
    plt.xlabel('Number of clusters')
    plt.ylabel('SSE (Inertia)')
    plt.grid(True)
    plt.show()
    plt.savefig('../plots/elbow_method_plot.png')

if __name__ == "__main__":
    logging.info("---------------Starting clustering model training----------------")
    train_clustering_model(n_clusters=3) # Adjust the number of clusters as needed
    logging.info("---------------Clustering model training completed successfully----------------")