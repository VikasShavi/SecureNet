import pandas as pd
import json
import datetime as dt
from time import sleep
from kafka import KafkaProducer

# Initialize Kafka Producer Client
producer = KafkaProducer(bootstrap_servers=['localhost:9092'])
print(f'Initialized Kafka producer at {dt.datetime.utcnow()}')

COLUMNS = ["StartTime", "Dur", "TotPkts", "TotBytes", "SrcBytes", "Proto_tcp", "Proto_udp", "Dir_one",
           "sTosone", "Proto_others", "Dir_others", "Proto", "SrcAddr", "Sport", "Dir", "DstAddr", 
           "Dport", "State", "sTos", "dTos", "Label"]
# Set a basic message counter and define the file path
counter = 0
file = "combined/noprepro.csv"

for chunk in pd.read_csv(file,encoding='unicode_escape',chunksize=10):

    # For each chunk, convert the invoice date into the correct time format
    chunk["StartTime"] = pd.to_datetime(chunk["StartTime"])
    # Reset the index for the chunk starting from 0
    chunk.reset_index(drop=True, inplace=True)
    # Set the counter as the message key
    key = str(counter).encode()
    # Convert the data frame chunk into a dictionary
    chunkd = chunk.to_dict()
    # print(chunkd)
    # Encode the dictionary into a JSON Byte Array
    data = json.dumps(chunkd, default=str).encode('utf-8')
    print(data)
    print("-"*75)
    # Send the data to Kafka
    producer.send(topic="logs", key=key, value=data)

    # Sleep to simulate a real-world interval
    sleep(1)
    
    # Increment the message counter for the message key
    counter = counter + 1

    print(f'Sent record to topic at time {dt.datetime.utcnow()}')



#################################################################################################


# import pandas as pd
# import json
# import datetime as dt
# from time import sleep
# from kafka import KafkaProducer

# # Initialize Kafka Producer Client
# producer = KafkaProducer(bootstrap_servers=['localhost:9092'])
# print(f'Initialized Kafka producer at {dt.datetime.utcnow()}')

# # Set a basic message counter and define the file path
# counter = 0
# file = "combined/noprepro.csv"

# for chunk in pd.read_csv(file, encoding='unicode_escape', chunksize=10):

#     # For each row in the chunk
#     for _, row in chunk.iterrows():
#         # Set the counter as the message key
#         key = str(counter).encode()
#         # Convert the row to a dictionary
#         row_dict = row.to_dict()
#         # Convert the dictionary into a JSON Byte Array
#         data = json.dumps(row_dict, default=str).encode('utf-8')
#         print(data)
#         print("-" * 75)
#         # Send the data to Kafka
#         producer.send(topic="logs", key=key, value=data)

#     # Sleep to simulate a real-world interval
#     sleep(5)

#     # Increment the message counter for the message key
#     counter += 1

#     print(f'Sent records to topic at time {dt.datetime.utcnow()}')
