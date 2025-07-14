import streamlit as st
import pandas as pd
import numpy as np
import joblib

regression_model = joblib.load('models/regression_model.pkl')
clustering_model = joblib.load('models/clustering_model.pkl')
scaler = joblib.load('models/scaler.pkl')

# List of features
features = [
    'brand', 'purchaseprice', 'volume', 'actual_price',
    'total_purchase_quantity', 'total_purchase_dollars',
    'totalsalesdollars', 'totalsalesprice', 'totalsalesquantity',
    'totalexcisetax', 'freightcost', 'stockturnover',
    'salestopurchaseratio', 'profit_per_unit', 
    'freight_burden', 'unitpurchaseprice', 'unsold_inventory_value'
]

st.set_page_config(page_title="Vendor Performance Analysis", layout="wide")
st.title("Vendor Profit Margin and Segmentation App 🛍️")
st.markdown("Enter vendor data to predict profit margin and segment vendors")

with st.form("vendor_input_form"):
    user_input = {}
    cols = st.columns(4)

    for i, feature in enumerate(features):
        with cols[i % 4]:  # 👈 Rotate through 4 column
            if feature == 'brand':
                value = st.number_input(f"{feature.replace('_', ' ').title()}", value=0, format="%d", step=1)
            else:
                value = st.number_input(f"{feature.replace('_', ' ').title()}", value=0.0, step=0.1)
            user_input[feature] = value


    submitted = st.form_submit_button("🔮 Predict")



if submitted:
    input_df = pd.DataFrame([user_input])
    
    # Scale the input data
    scaled_input = scaler.transform(input_df)
    
    # Predict profit margin
    predicted_margin = regression_model.predict(scaled_input)[0]
    
    # Predict cluster
    cluster_label = clustering_model.predict(scaled_input)[0]
    
    # Display results
    st.subheader("Predicted Profit Margin")
    st.success(f"💰 The predicted profit margin is: {(predicted_margin):.2f}%")
    st.subheader("Vendor Cluster")
    st.success(f"📦 The vendor belongs to cluster: {cluster_label}")
    
    # Display the input data
    st.subheader("🔍 Input Summary")
    st.write(input_df)