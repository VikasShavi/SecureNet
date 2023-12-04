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

* Clone the repository:

```bash
git clone https://github.com/yourusername/SecurNet.git
cd SecurNet
```
* Then download the files required from here, [LINK](https://iitgoffice-my.sharepoint.com/:f:/g/personal/v_shavi_iitg_ac_in/EipXYnDl2_VOm3tJh03EbswBaUhZxTKsDsLQEB9q40NKrg?e=wKbOVs) and move it to the SecureNet folder.

## Model Training
The preproccessing.py file cleans and makes the raw log data ready for training. It outputs prepro.csv file. This processed log data is used by MLmodeltraining.py file to train the model.

* First run the preprocessing.py file
* It will generate a csv file in folder named outprepro.
* Change the name of the csv file to prepro.csv
* Now run the MLmodeltraining.py file. This will save the model in Model folder ready to be used.

## Network Intrusion Detection
Here we will simulate log data coming in realtime. I am reading a csv file of raw log data and sending it in chunks of 10 rows to Kafka.
Flow of the log data can be seen below:
![Screenshot 2023-12-01 at 18 57 38-PhotoRoom png-PhotoRoom](https://github.com/chiranjibsuriitg/DA331fall23_200121062/assets/83757578/0003843a-4ed3-49d8-bad6-f2cd779a2083)
Running the project, follow the steps below,
NOTE: Run all the individual commands in a separate terminal.
1. Run Apache zookeeper and kafka in different terminals one after the other by following commnads:
```bash
zookeeper-server-start /opt/homebrew/etc/zookeeper/zoo.cfg
kafka-server-start /opt/homebrew/etc/kafka/server.properties
```
2. Create Kafka topics, "logs", "logsprocessed" and logslabelled"
```bash
kafka-topics --create --topic logs --bootstrap-server localhost:9092
kafka-topics --create --topic logsprocessed --bootstrap-server localhost:9092
kafka-topics --create --topic logslabelled --bootstrap-server localhost:9092
```
3. Start Apache Pinot Controller, Broker and Server
```bash
pinot-admin StartController -zkAddress localhost:2181 -clusterName PinotCluster -controllerPort 9001
pinot-admin StartBroker -zkAddress localhost:2181 -clusterName PinotCluster -brokerPort 7001
pinot-admin StartServer -zkAddress localhost:2181 -clusterName PinotCluster -serverPort 8001 -serverAdminPort 8011
```
4. Send the table schema and table config to Apache Pinot.
```bash
pinot-admin AddTable \
    -schemaFile files_config/transcript_schema.json \
    -tableConfigFile files_config/transcript_table_realtime.json \
    -controllerPort 9001 -exec
```
6. Start 0.py, 1.py, 2.py in three separate terminals one after the other

7. Open the apache pinot dashboard to see data ingesting ----> [Link](http://localhost:9001)

8. Run streamlit app to see the dashboard
```bash
streamlit run app.py
```

## Screenshot
This is how your dashboard will look like...😁
![screenshot (6)](https://github.com/chiranjibsuriitg/DA331fall23_200121062/assets/83757578/361cd67d-bae4-4c20-8822-497b9e90ee2b)
