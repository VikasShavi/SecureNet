# Import necessary libraries
from kafka import KafkaConsumer, KafkaProducer  # KafkaConsumer and KafkaProducer for interacting with Apache Kafka
import json  # JSON for handling JSON data
import datetime as dt  # Datetime for working with dates and times
from pyspark.sql import SparkSession  # SparkSession for working with Apache Spark
from pyspark.sql.types import DoubleType, StringType  # Data types for Spark DataFrame columns
from pyspark.ml.feature import MinMaxScaler, VectorAssembler  # Spark MLlib for machine learning operations
from pyspark.ml import Pipeline  # Spark MLlib for creating a pipeline of operations
from pyspark.sql.functions import col, udf, expr, regexp_replace, when, lit  # Functions for Spark DataFrame operations
from pyspark.sql import Row  # Row for representing a row of data in a Spark DataFrame

# Define a list of column names
COLUMNS_LIST = ["StartTime", "Dur", "TotPkts", "TotBytes", "SrcBytes", "Proto_tcp", "Proto_udp", "Dir_one",
           "sTosone", "Proto_others", "Dir_others", "Proto", "SrcAddr", "Sport", "Dir", "DstAddr", 
           "Dport", "State", "sTos", "dTos", "Label"]

# Define a function for data preprocessing
def doingwork(df):
    """------------------------------------------------------------------------------------------------"""
    # Convert labels to numerical values
    df = df.withColumn("Label", when(df["Label"].contains("Background"), "0").otherwise(df["Label"]))
    df = df.withColumn("Label", when(df["Label"].contains("Normal"), "2").otherwise(df["Label"]))
    df = df.withColumn("Label", when(df["Label"].contains("Botnet"), "1").otherwise(df["Label"]))

    """------------------------------------------------------------------------------------------------"""
    # Create binary columns for unique values in "Proto" and "Dir" columns
    unique_prot_list = df.select("Proto").distinct().rdd.flatMap(lambda x: x).collect()
    for proto_value in unique_prot_list:
        column_name = f"Proto_{proto_value}"
        df = df.withColumn(column_name, when(df["Proto"] == proto_value, 1).otherwise(0))

    unique_dir_list = df.select("Dir").distinct().rdd.flatMap(lambda x: x).collect()
    for dir_value in unique_dir_list:
        column_name = f"Dir_{dir_value.strip()}"
        df = df.withColumn(column_name, when(df["Dir"] == dir_value, 1).otherwise(0))

    # Handle missing values and cast "sTos" column to DoubleType
    df = df.withColumn("sTos", df["sTos"].cast(DoubleType()))
    medianvalue = df.approxQuantile("sTos", [0.5], 0.25)[0]
    df = df.fillna(medianvalue, subset=["sTos"])
    df = df.withColumn("sTos", df["sTos"].cast(StringType()))
    
    # Create binary columns for unique values in "sTos" column
    unique_sTos_list = df.select("sTos").distinct().rdd.flatMap(lambda x: x).collect()
    unique_sTos_list = ["1.0"]
    for stos_value in unique_sTos_list:
        column_name = f"sTos_{stos_value.strip()}"
        df = df.withColumn(column_name, when(df["sTos"] == stos_value, 1).otherwise(0))

    # Drop unnecessary columns and rename one of them
    cols_to_drop = ["sTos_0.0", "sTos_192.0", "sTos_2.0", "sTos_3.0"]
    df = df.drop(*cols_to_drop)
    df = df.withColumnRenamed("sTos_1.0", "sTosone")
    """------------------------------------------------------------------------------------------------"""

    # Handle specific cases related to "Proto" column
    protocol_columns = [f"Proto_{i}" for i in unique_prot_list]
    if "Proto_tcp" in protocol_columns:
        protocol_columns.remove("Proto_tcp")
    else:
        new_column_name = "Proto_tcp"
        position = 5
        if new_column_name not in df.columns:
            new_df = df.select(df.columns[:position] + [lit(0).alias(new_column_name)] + df.columns[position:])
            df = new_df

    if "Proto_udp" in protocol_columns:
        protocol_columns.remove("Proto_udp")
    else:
        new_column_name = "Proto_udp"
        position = 6
        if new_column_name not in df.columns:
            new_df = df.select(df.columns[:position] + [lit(0).alias(new_column_name)] + df.columns[position:])
            df = new_df

    # Create a new column for other protocols based on existing columns
    if len(protocol_columns) > 0:
        condition = when(col(protocol_columns[0]) == 1, 1)
        for protocol_column in protocol_columns[1:]:
            condition = condition.when(col(protocol_column) == 1, 1)
        df = df.withColumn("Proto_others", condition.otherwise(0))
        df = df.drop(*protocol_columns)
    else:
        new_column_name = "Proto_others"
        position = 9
        if new_column_name not in df.columns:
            new_df = df.select(df.columns[:position] + [lit(0).alias(new_column_name)] + df.columns[position:])
            df = new_df
    """------------------------------------------------------------------------------------------------"""

    # Handle specific cases related to "Dir" column
    dir_columns = [f"Dir_{i.strip()}" for i in unique_dir_list]
    if "Dir_<->" in dir_columns:
        dir_columns.remove("Dir_<->")
    else:
        new_column_name = "Dir_<->"
        position = 7
        if new_column_name not in df.columns:
            new_df = df.select(df.columns[:position] + [lit(0).alias(new_column_name)] + df.columns[position:])
            df = new_df

    # Create a new column for other directions based on existing columns
    if len(dir_columns) > 0:
        condition = when(col(dir_columns[0]) == 1, 1)
        for dir_column in dir_columns[1:]:
            condition = condition.when(col(dir_column) == 1, 1)
        df = df.withColumn("Dir_others", condition.otherwise(0))
        df = df.drop(*dir_columns)
    else:
        new_column_name = "Dir_others"
        position = 10
        if new_column_name not in df.columns:
            new_df = df.select(df.columns[:position] + [lit(0).alias(new_column_name)] + df.columns[position:])
            df = new_df
    """------------------------------------------------------------------------------------------------"""

    # Rename a column and select the desired columns
    df = df.withColumnRenamed("Dir_<->", "Dir_one")
    df = df.select(COLUMNS_LIST)
    return df

# Initialize Kafka consumer
consumer = KafkaConsumer('logs',
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
    """------------------------------------------------------------------------------------------------"""
    # Extract data from Kafka message and create a Spark DataFrame
    rows = [Row(**{key: value[str(idx)] for key, value in message.value.items()}) for idx in range(len(message.value['StartTime']))]
    spark = SparkSession.builder.appName("example").getOrCreate()
    df = spark.createDataFrame(rows)

    # Perform data preprocessing using the defined function
    df_combined_two = doingwork(df)

    # Collect the rows of the processed DataFrame
    rows = df_combined_two.collect()
    """------------------------------------------------------------------------------------------------"""

    # Create a dictionary to store the processed data
    result_dict = {}
    for col_num in df_combined_two.columns:
        column_dict = {}
        for idx, row in enumerate(rows):
            column_dict[idx] = row[col_num]
        result_dict[str(col_num)] = column_dict

    # Convert the dictionary to a JSON-encoded byte string
    chunkd = result_dict
    data = json.dumps(chunkd, default=str).encode('utf-8')
    
    # Print the data and send it to Kafka
    print(f"DATA: {data}")
    producer.send(topic="logsprocessed", value=data)
    
    # Stop the Spark session
    spark.stop()
    print("-" * 50)
