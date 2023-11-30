from kafka import KafkaConsumer, KafkaProducer
import json
import datetime as dt
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, StringType
from pyspark.ml.feature import MinMaxScaler, VectorAssembler
from pyspark.ml import Pipeline
from pyspark.sql.functions import col, udf, expr, regexp_replace, when, lit
from pyspark.sql import Row

COLUMNS_LIST = ["StartTime", "Dur", "TotPkts", "TotBytes", "SrcBytes", "Proto_tcp", "Proto_udp", "Dir_one",
           "sTosone", "Proto_others", "Dir_others", "Proto", "SrcAddr", "Sport", "Dir", "DstAddr", 
           "Dport", "State", "sTos", "dTos", "Label"]

def doingsex(df):
    """------------------------------------------------------------------------------------------------"""
    # cols_to_drop = ["SrcAddr", "Sport", "DstAddr", "Dport", "State", "dTos"]
    # storing_time = df.select("StartTime")
    # df = df.drop(*cols_to_drop)

    """------------------------------------------------------------------------------------------------"""

    # Encoding Label to Integers
    df = df.withColumn("Label", when(df["Label"].contains("Background"), "0").otherwise(df["Label"]))
    df = df.withColumn("Label", when(df["Label"].contains("Normal"), "2").otherwise(df["Label"]))
    df = df.withColumn("Label", when(df["Label"].contains("Botnet"), "1").otherwise(df["Label"]))

    """------------------------------------------------------------------------------------------------"""

    # Hot encoding Proto
    # unique_protos = ['tcp', 'udp', 'rtp', 'pim', 'icmp', 'arp', 'ipx/spx', 'rtcp', 'igmp', 'ipv6-icmp', 'ipv6', 'udt', 'esp', 'unas', 'rarp']
    unique_prot_list = df.select("Proto").distinct().rdd.flatMap(lambda x: x).collect()
    for proto_value in unique_prot_list:
        column_name = f"Proto_{proto_value}"
        df = df.withColumn(column_name, when(df["Proto"] == proto_value, 1).otherwise(0))

    # Hot encoding Dir
    unique_dir_list = df.select("Dir").distinct().rdd.flatMap(lambda x: x).collect()
    for dir_value in unique_dir_list:
        column_name = f"Dir_{dir_value.strip()}"
        df = df.withColumn(column_name, when(df["Dir"] == dir_value, 1).otherwise(0))

    # Median imputing sTos
    df = df.withColumn("sTos", df["sTos"].cast(DoubleType()))
    medianvalue = df.approxQuantile("sTos", [0.5], 0.25)[0]
    df = df.fillna(medianvalue, subset=["sTos"])

    # Hot encoding sTos
    df = df.withColumn("sTos", df["sTos"].cast(StringType()))
    unique_sTos_list = df.select("sTos").distinct().rdd.flatMap(lambda x: x).collect()
    unique_sTos_list = ["1.0"]
    # Create new columns and fill with 0s and 1s
    for stos_value in unique_sTos_list:
        column_name = f"sTos_{stos_value.strip()}"
        df = df.withColumn(column_name, when(df["sTos"] == stos_value, 1).otherwise(0))
    # cols_to_drop = ["sTos", "Dir", "Proto"]
    # df = df.drop(*cols_to_drop)
    cols_to_drop = ["sTos_0.0", "sTos_192.0", "sTos_2.0", "sTos_3.0"]
    df = df.drop(*cols_to_drop)
    df = df.withColumnRenamed("sTos_1.0", "sTosone")
    """------------------------------------------------------------------------------------------------"""

    # List of protocol columns
    protocol_columns = [f"Proto_{i}" for i in unique_prot_list]
    if "Proto_tcp" in protocol_columns:
        protocol_columns.remove("Proto_tcp")
    else:
        # Specify the column name and position
        new_column_name = "Proto_tcp"
        position = 5
        # Check if the column is already present
        if new_column_name not in df.columns:
            # If not present, create a new DataFrame with the column added at the desired position
            new_df = df.select(df.columns[:position] + [lit(0).alias(new_column_name)] + df.columns[position:])
            df = new_df

    if "Proto_udp" in protocol_columns:
        protocol_columns.remove("Proto_udp")
    else:
        # Specify the column name and position
        new_column_name = "Proto_udp"
        position = 6
        # Check if the column is already present
        if new_column_name not in df.columns:
            # If not present, create a new DataFrame with the column added at the desired position
            new_df = df.select(df.columns[:position] + [lit(0).alias(new_column_name)] + df.columns[position:])
            df = new_df
    # Initialize the when condition with the otherwise condition
    if len(protocol_columns) > 0:
        condition = when(col(protocol_columns[0]) == 1, 1)

        # Build the when conditions dynamically
        for protocol_column in protocol_columns[1:]:
            condition = condition.when(col(protocol_column) == 1, 1)

        # Apply the conditions and create a new column "Proto_others"
        df = df.withColumn("Proto_others", condition.otherwise(0))
        # Drop the original protocol columns
        df = df.drop(*protocol_columns)
    else:
        # Specify the column name and position
        new_column_name = "Proto_others"
        position = 9
        # Check if the column is already present
        if new_column_name not in df.columns:
            # If not present, create a new DataFrame with the column added at the desired position
            new_df = df.select(df.columns[:position] + [lit(0).alias(new_column_name)] + df.columns[position:])
            df = new_df
    # df_combined.show(5)
    """------------------------------------------------------------------------------------------------"""

    # dir_columns = ['Dir_<-', 'Dir_who', 'Dir_?>', 'Dir_->', 'Dir_<?', 'Dir_<?>']
    # Combine specified columns into a new 'protocol' column
    dir_columns = [f"Dir_{i.strip()}" for i in unique_dir_list]
    if "Dir_<->" in dir_columns:
        dir_columns.remove("Dir_<->")
    else:
        # Specify the column name and position
        new_column_name = "Dir_<->"
        position = 7
        # Check if the column is already present
        if new_column_name not in df.columns:
            # If not present, create a new DataFrame with the column added at the desired position
            new_df = df.select(df.columns[:position] + [lit(0).alias(new_column_name)] + df.columns[position:])
            df = new_df
    if len(dir_columns) > 0:
        # Initialize the when condition with the otherwise condition
        condition = when(col(dir_columns[0]) == 1, 1)

        # Build the when conditions dynamically
        for dir_column in dir_columns[1:]:
            condition = condition.when(col(dir_column) == 1, 1)

        # Apply the conditions and create a new column "Proto_others"
        df = df.withColumn("Dir_others", condition.otherwise(0))
        df = df.drop(*dir_columns)
    else:
        # Specify the column name and position
        new_column_name = "Dir_others"
        position = 10
        # Check if the column is already present
        if new_column_name not in df.columns:
            # If not present, create a new DataFrame with the column added at the desired position
            new_df = df.select(df.columns[:position] + [lit(0).alias(new_column_name)] + df.columns[position:])
            df = new_df
    """------------------------------------------------------------------------------------------------"""

    df = df.withColumnRenamed("Dir_<->", "Dir_one")
    # print(df.printSchema())
    df = df.select(COLUMNS_LIST)
    return df

# Consume all the messages from the topic but do not mark them as 'read' (enable_auto_commit=False)
# so that we can re-read them as often as we like.
consumer = KafkaConsumer('logs',
                         group_id='test-consumer-group',
                         bootstrap_servers=['localhost:9092'],
                         value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                         auto_offset_reset='earliest',
                         enable_auto_commit=True)
# print(consumer.)

# Initialize Kafka Producer Client
producer = KafkaProducer(bootstrap_servers=['localhost:9092'])
print(f'Initialized Kafka producer at {dt.datetime.utcnow()}')
for message in consumer:
    """------------------------------------------------------------------------------------------------"""
    # Convert the dictionary to a list of Row objects
    rows = [Row(**{key: value[str(idx)] for key, value in message.value.items()}) for idx in range(len(message.value['StartTime']))]
    # Create a Spark session
    spark = SparkSession.builder.appName("example").getOrCreate()
    df = spark.createDataFrame(rows)
    df_combined_two = doingsex(df)

    rows = df_combined_two.collect()
    """------------------------------------------------------------------------------------------------"""
    # Initialize an empty dictionary to store the final result
    result_dict = {}
    # Iterate through the columns and construct the dictionary
    for col_num in df_combined_two.columns:
        # Create a nested dictionary for each column
        column_dict = {}
        # Iterate through the rows and populate the inner dictionary
        for idx, row in enumerate(rows):
            column_dict[idx] = row[col_num]
        # Add the inner dictionary to the outer dictionary
        result_dict[str(col_num)] = column_dict
    chunkd = result_dict
    # Encode the dictionary into a JSON Byte Array
    data = json.dumps(chunkd, default=str).encode('utf-8')
    print(f"DATA: {data}")
    # Send the data to Kafka
    producer.send(topic="logsprocessed", value=data)
    spark.stop()
    print("-"*50)