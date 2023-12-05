# Import necessary libraries
from kafka import KafkaConsumer, KafkaProducer  # KafkaConsumer and KafkaProducer for interacting with Apache Kafka
import datetime as dt  # Datetime for working with dates and times
import json  # JSON for handling JSON data
import pandas as pd  # Pandas for data manipulation and analysis
import numpy as np  # NumPy for numerical operations
import joblib  # Joblib for saving and loading models
from sklearn.metrics import accuracy_score, classification_report  # Scikit-learn for machine learning metrics
from xgboost.spark import SparkXGBClassifierModel  # XGBoost for Spark MLlib
from pyspark.sql import SparkSession, Row  # SparkSession for working with Apache Spark, Row for representing a row of data
from pyspark.ml.feature import VectorAssembler  # Spark MLlib for feature engineering
from pyspark.sql.functions import lit, col  # Functions for Spark DataFrame operations
from xgboost.spark import SparkXGBClassifier  # XGBoost for Spark MLlib
from pyspark.ml.evaluation import MulticlassClassificationEvaluator  # Spark MLlib for model evaluation

# Define a list of column names
COLUMNS_LIST = ["StartTime", "Dur", "TotPkts", "TotBytes", "SrcBytes", "Proto_tcp", "Proto_udp", "Dir_one",
           "sTosone", "Proto_others", "Dir_others", "Proto", "SrcAddr", "Sport", "Dir", "DstAddr", 
           "Dport", "State", "sTos", "dTos", "Label"]

# Define a function for applying a machine learning model to the data
def doingtty(df):
    # Specify the path to the saved XGBoost model
    model_save_path = "Model/pyspark"
    
    # Load the XGBoost model
    loaded_model = SparkXGBClassifierModel.load(model_save_path)
    
    # Define the feature columns for the model
    feature_cols = ['Dur', 'TotPkts', 'TotBytes', 'SrcBytes', 'Proto_tcp', 'Proto_udp', 'Dir_one', 'sTosone', 'Proto_others', 'Dir_others']
    
    # Create a VectorAssembler for feature engineering
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
    df_new = assembler.transform(df)
    
    # Select relevant columns for testing
    selected_cols = ["StartTime", "features", "Label"]
    df_test = df_new.select(selected_cols)
    
    # Cast the "Label" column to integer type
    df_test = df_test.withColumn("Label", col("Label").cast('int'))

    # Make predictions using the loaded model
    predictions = loaded_model.transform(df_test)
    
    # Evaluate the model accuracy
    evaluator = MulticlassClassificationEvaluator(
        labelCol="Label",
        predictionCol="prediction",
        metricName="accuracy"
    )
    accuracy = evaluator.evaluate(predictions)
    print(f"Accuracy: {accuracy}")
    
    # Select relevant columns for the final DataFrame
    predictions = predictions.select(["StartTime", "prediction"])
    predictions = predictions.withColumn("prediction", col("prediction").cast('int'))
    df_final = df.join(predictions, on='StartTime')
    final_cols = ["StartTime", "Dur", "TotPkts", "TotBytes", "SrcBytes", "Proto_tcp", "Proto_udp", "Dir_one",
           "sTosone", "Proto_others", "Dir_others", "Proto", "SrcAddr", "Sport", "Dir", "DstAddr", 
           "Dport", "State", "sTos", "dTos", "prediction", "Label"]
    df_final = df_final.select(final_cols)
    return df_final

# Initialize Kafka consumer
consumer = KafkaConsumer('logsprocessed',
                         group_id='test-consumer-group',
                         bootstrap_servers=['localhost:9092'],
                         value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                         auto_offset_reset='earliest',
                         enable_auto_commit=True)

# Initialize Kafka producer
producer = KafkaProducer(bootstrap_servers=['localhost:9092'])
print(f'Initialized Kafka producer at {dt.datetime.utcnow()}')

# Loop through Kafka messages
for message in consumer:
    rows = [Row(**{key: value[str(idx)] for key, value in message.value.items()}) for idx in range(len(message.value['StartTime']))]
    spark = SparkSession.builder.appName("example").getOrCreate()
    df = spark.createDataFrame(rows)
    
    # Apply the machine learning model using the defined function
    combined_df = doingtty(df)
    
    # Extract the first row of the resulting DataFrame
    row = combined_df.first()
    row_dict = row.asDict()
    
    # Convert the row to a JSON-encoded byte string
    data = json.dumps(row_dict, default=str).encode('utf-8')
    
    # Print the data and send it to Kafka
    print(data)
    print("-" * 75)
    producer.send(topic="logslabelled", value=data)
