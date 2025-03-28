# Sensor Data Assessment Pipeline

This project is a streaming data pipeline built using Apache Flink and Apache Kafka. It ingests real-time sensor data, filters and aggregates it, then sends the results to a Kafka output topic.

## **Input Data**

The pipeline processes sensor data in the following format:

| Column | Description | Example |
| --- | --- | --- |
| `date` | Date in `yyyy-MM-dd` format | `2004-03-31` |
| `time` | Time in `HH:mm:ss.SSSSSS` format | `03:38:15.757551` |
| `epoch` | Epoch ID for each sensor | `2` |
| `moteId` | Sensor device ID (1-54) | `37` |
| `temperature` | Sensor temperature (°C) | `122.153` |
| `humidity` | Sensor humidity (%) | `-3.91901` |
| `light` | Sensor light intensity (Lux) | `11.04` |
| `voltage` | Sensor voltage (V) | `2.03397` |

## **How It Works**

1. **Input**:  Use Apache Kafka to generate data flow.
2. **Parsing**: Converts raw text into structured `SensorData` objects.
3. **Timestamp Assignment**: Assigns event time to each record based on the `date` and `time` fields.
4. **Anomaly Detection**: Filters out normal readings and flags anomalies (e.g., high temperature or fluctuating voltage).
5. **Window Aggregation**: Aggregates anomalies for each device in 10-second tumbling time windows.

## **Running the Pipeline**

### Prerequisites

- Apache Flink (version 1.20 or compatible)
- Kafka
  - Broker running at: 10.10.2.61:9092
  - Required topics:
    - sensor-data (input)
    - processed-data (output)
### Steps

1. Produce data by running the python script:

    ```bash
    python send_sensor_data_kafka.py
    ```

2. Build the project with Maven:

    ```bash
    mvn clean install
    ```
3. Start Kafka, Flink job manager and task managers.

4. Run Flink Job

    ```bash
    bin/flink run -c sensorassessment.SensorAssessmentJob \
    target/sensor-assessment-0.1.jar \
    --input-topic sensor-data \
    --output-topic processed-data \
    --bootstrap.servers 10.10.2.61:9092 \
    --window-size 30 \
    --temperature-threshold 80
    ```


---
