from kafka import KafkaConsumer
import json
import pandas as pd

COLUMNS_LIST = ["StartTime", "Dur", "TotPkts", "TotBytes", "SrcBytes", "Proto_tcp", "Proto_udp", "Dir_one",
           "sTosone", "Proto_others", "Dir_others", "Proto", "SrcAddr", "Sport", "Dir", "DstAddr", 
           "Dport", "State", "sTos", "dTos", "prediction", "Label"]

# To consume latest messages and auto-commit offsets
consumer = KafkaConsumer('logslabelled',
                         group_id='test-consumer-group',
                         bootstrap_servers=['localhost:9092'],
                         value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                         auto_offset_reset='earliest',
                         enable_auto_commit=True)

# Flag to check if columns are already written
columns_written = False

# Iterate over messages
for message in consumer:
    try:
        print(f"{json.dumps(message.value)}\n--------------------recieved-------------------------")
    except Exception as e:
        print(f"Error processing message: {str(e)}")


