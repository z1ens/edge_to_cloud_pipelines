# -*- coding: utf-8 -*-
import pandas as pd
from time import sleep
from kafka import KafkaProducer
import os

# Kafka set up
KAFKA_TOPIC = 'sensor-data'
KAFKA_SERVER = '10.10.2.61:9092:9092' # run on node zf01
CSV_FILE = 'cleaned_data.csv'
OFFSET_STEP_MS = 10000  # 10s differentce for each iteration
INTER_RECORD_GAP_MS = 100  
SLEEP_SECONDS = 10

# initialise Kafka Producer，send CSV format
producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    value_serializer=lambda v: v.encode('utf-8')
)

# safely transfer int type
def safe_int(val):
    return str(int(val)) if val != "" else ""

# initialise
base_timestamp = None
iteration = 0


while True:
    if not os.path.exists(CSV_FILE):
        print(f"Could not find file：{CSV_FILE}")
        break

    # get CSV data
    df = pd.read_csv(CSV_FILE)

    # set up timestamp
    if base_timestamp is None:
        base_timestamp = df['timestamp'].max()

    # linear growth of the timestamp
    offset = iteration * OFFSET_STEP_MS
    df['timestamp'] = base_timestamp + offset + df.index * INTER_RECORD_GAP_MS

    # Transfer NaN to ""，flink would take it as null
    df = df.fillna("")

    # transfer to CSV format
    for _, row in df.iterrows():
        row_list = row.tolist()
        row_list[0] = safe_int(row_list[0])  # timestamp
        row_list[1] = safe_int(row_list[1])  # epoch
        row_list[2] = safe_int(row_list[2])  # moteid
        for i in range(3, len(row_list)):
            row_list[i] = str(row_list[i])
        line = ','.join(row_list)
        producer.send(KAFKA_TOPIC, line)

    producer.flush()
    print(f"[✓] successfully sent iteration {iteration + 1} data（with offset {offset} ms）")

    iteration += 1
    sleep(SLEEP_SECONDS)
