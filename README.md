# 📦 Supplier Performance Analytics : Profitability Prediction and Interactive Dashboards for Procurement Strategy  

## 📁 Domain  
**Retail Procurement & Supply Chain Optimization**

## 🧠 Project Overview  
This project aims to build a full-fledged Supplier Performance Analytics platform to drive data-informed procurement decisions. It includes data ingestion, ETL automation, ML-driven vendor segmentation, and interactive dashboards to enable strategic insights for procurement teams.

---

## 🎯 Business Problem  

- Identify low-performing vendors negatively affecting profit margins  
- Understand product-level profitability and freight cost impact  
- Detect operational inefficiencies in procurement and supplier deliveries  
- Predict future vendor risk using historical performance  
- Segment vendors for targeted negotiation and engagement strategies  

---

## 🛠️ End-to-End Workflow  

### 1. 📦 Data Collection  
Collected supplier, sales, freight, and product data to simulate a multi-vendor environment.

### 2. 🔄 Data Ingestion Pipeline  
- **Tech**: Apache Airflow (Dockerized), Astro Cloud  
- **Task**: Load raw CSVs into PostgreSQL database via automated DAGs  

### 3. 📊 EDA with SQL  
- Performed deep analysis using SQL queries directly on PostgreSQL  
- Uncovered cost anomalies, freight issues, vendor delivery patterns  

### 4. ⚙️ ETL Pipeline  
- Built modular ETL using Apache Airflow DAGs  
- Tasks include: data cleaning, standardization, enrichment, KPI derivation  

### 5. 📈 Performance Analysis (Python)  
- Tools: pandas, seaborn, matplotlib  
- Visual analysis of profit margins, freight cost vs. sales, vendor ranking  

### 6. 🤖 ML Integration  
- **Models Used**:  
  - *Logistic Regression* for Vendor Profitability Classification  
  - *K-Means Clustering* for Vendor Segmentation  
- **Deployment**: Streamlit Web App for stakeholder interaction 
url: https://vendorperformanceprediction.streamlit.app/

### 7. 📊 Power BI Dashboards  
- Pages include:  
  - Vendor Performance Overview  
  - Product-Level Profitability & Freight Impact  
  - Operational Efficiency & KPIs  
  - Strategic Q&A via Power BI Copilot  
  - Predictive Insights for Future Strategy  

---

## 🧰 Tech Stack  

| Category | Tools Used |
|---------|-------------|
| Language | Python, SQL |
| Data Processing | pandas, seaborn, matplotlib |
| Database | PostgreSQL |
| Orchestration | Apache Airflow (Dockerized) |
| Platform | Astro Cloud |
| Machine Learning | scikit-learn |
| Deployment | Streamlit |
| Visualization | Power BI |

---

## 📬 Contact  
Feel free to connect or reach out for collaboration or feedback:  
**Pranoy Chakraborty** | [LinkedIn](https://www.linkedin.com/in/pranoychakraborty)

---

## 📌 Project Status  
✅ Completed — Ready for portfolio/demo use.  
📈 Actively open to enhancements like anomaly detection and time-series forecasting.

