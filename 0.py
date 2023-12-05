# Import necessary libraries
import pandas as pd  # Pandas for data manipulation and analysis
import json  # JSON for handling JSON data
import datetime as dt  # Datetime for working with dates and times
from time import sleep  # Sleep for introducing delays
from kafka import KafkaProducer  # KafkaProducer for interacting with Apache Kafka

# Initialize Kafka producer with the specified bootstrap servers
producer = KafkaProducer(bootstrap_servers=['localhost:9092'])
print(f'Initialized Kafka producer at {dt.datetime.utcnow()}')

# Define the columns of the data
COLUMNS = ["StartTime", "Dur", "TotPkts", "TotBytes", "SrcBytes", "Proto_tcp", "Proto_udp", "Dir_one",
           "sTosone", "Proto_others", "Dir_others", "Proto", "SrcAddr", "Sport", "Dir", "DstAddr", 
           "Dport", "State", "sTos", "dTos", "Label"]

# Initialize a counter variable
counter = 0

# Specify the file path
file = "combined/data.csv"

# Loop through the CSV file in chunks of 10 rows
for chunk in pd.read_csv(file, encoding='unicode_escape', chunksize=10):
    # Convert the "StartTime" column to datetime format
    chunk["StartTime"] = pd.to_datetime(chunk["StartTime"])
    
    # Reset the index of the chunk
    chunk.reset_index(drop=True, inplace=True)
    
    # Generate a key for Kafka as a string-encoded counter
    key = str(counter).encode()
    
    # Convert the chunk to a dictionary
    chunk_dict = chunk.to_dict()
    
    # Convert the dictionary to a JSON-encoded byte string
    data = json.dumps(chunk_dict, default=str).encode('utf-8')
    
    # Print the data and a separator line
    print(data)
    print("-" * 75)
    
    # Send the data to the Kafka topic with the specified key
    producer.send(topic="logs", key=key, value=data)
    
    # Introduce a small delay
    sleep(1)
    
    # Increment the counter
    counter = counter + 1
    
    # Print a message indicating that the record was sent to the topic
    print(f'Sent record to topic at time {dt.datetime.utcnow()}')

