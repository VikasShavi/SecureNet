# SecurNet Project

## Overview

SecurNet is a comprehensive network security project that leverages a Network Intrusion Detection System (NIDS) to enhance the security of networks. The project involves data preprocessing, feature selection, machine learning-based log classification, and a Streamlit dashboard for insightful visualization of key metrics.

## Workflow

### 1. Data Collection and Preprocessing

The project begins with the collection of network logs, which are sent to a Kafka topic named "logs" for initial preprocessing. The first Python file handles this task, preparing the data for feature selection.

### 2. Feature Selection and Further Preprocessing

A second Python file retrieves the preprocessed data from the "logs" Kafka topic, performs additional preprocessing, and sends the refined data to another Kafka topic named "logsprocessed."

### 3. Machine Learning-Based Log Classification

The third Python file retrieves data from the "logsprocessed" Kafka topic. It passes the logs through a trained machine learning model to classify them into categories: Background, Normal, or Botnet. The results are then sent to the "logslabelled" Kafka topic.

### 4. Data Storage with Apache Pinot

Apache Pinot acts as a consumer, ingesting data from the "logslabelled" Kafka topic and storing it in a database. This ensures efficient storage and retrieval of labeled log data.

### 5. Streamlit Dashboard

The final component is a Streamlit dashboard that fetches data from Apache Pinot. The dashboard displays key metrics and insights derived from the labeled log data. This visualization aids in better defending against network attacks by providing a real-time overview of network security.

## Getting Started

To set up and run the SecurNet project, follow these steps:

1. Clone the repository:

```bash
git clone https://github.com/yourusername/SecurNet.git
cd SecurNet
```
2. Run Apache zookeeper, kafka and pinot cluster, server, broker as given in commands.txt file

3. Start 0.py, 1.py, 2.py in three separate terminals one after the other

4. Open the apache pinot dashboard to see data ingesting ----> [Link](http://localhost:9001)
