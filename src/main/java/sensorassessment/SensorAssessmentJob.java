/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package sensorassessment;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import java.time.Duration;
import java.util.Properties;

public class SensorAssessmentJob {
	public static void main(String[] args) throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.disableOperatorChaining(); // must be before any operator is added

		ParameterTool parameters = ParameterTool.fromArgs(args);
		String inputTopic = parameters.get("input-topic", "sensor-data");
		String outputTopic = parameters.get("output-topic", "processed-data");
		String kafkaBrokers = parameters.get("bootstrap.servers", "10.10.2.61:9092");
		int threshold = parameters.getInt("temperature-threshold", 80);
		int windowSizeSec = parameters.getInt("window-size", 30);

		Properties kafkaConsumerProps = new Properties();
		kafkaConsumerProps.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBrokers);
		kafkaConsumerProps.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "sensor-test");
		kafkaConsumerProps.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

		Properties kafkaProducerProps = new Properties();
		kafkaProducerProps.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBrokers);
		kafkaProducerProps.setProperty(ProducerConfig.ACKS_CONFIG, "1");
		kafkaProducerProps.setProperty(ProducerConfig.RETRIES_CONFIG, "5");
		kafkaProducerProps.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "false");

		FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(inputTopic, new SimpleStringSchema(), kafkaConsumerProps);

		DataStream<String> rawStream = env.addSource(kafkaConsumer).name("Kafka Source");

		DataStream<SensorData> parsedStream = rawStream
				.map(SensorParser::parse)
				.name("Map[0]");

		DataStream<SensorData> filteredStream = parsedStream
				.filter(sensorData -> sensorData != null && sensorData.getTemperature() != null && sensorData.getTemperature() <= threshold)
				.name("Filter: Threshold");

		DataStream<SensorData> timestampedStream = filteredStream
				.map(new OperatorLatencyMapper("Map[1]"))
				.name("Map[1]")
				.assignTimestampsAndWatermarks(
						WatermarkStrategy.<SensorData>forBoundedOutOfOrderness(Duration.ofSeconds(1))
								.withTimestampAssigner((event, timestamp) -> {
									long ts = event.getTimestamp();
									if (ts < 1000000000000L) ts *= 1000;
									return ts;
								})
								.withIdleness(Duration.ofSeconds(10))
				)
				.name("Assign Timestamps");

		KeyedStream<SensorData, Integer> keyedStream = timestampedStream.keyBy(SensorData::getMoteId);

		WindowedStream<SensorData, Integer, TimeWindow> windowedStream =
				keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(windowSizeSec)));

		DataStream<SensorData> aggregated = windowedStream.reduce((ReduceFunction<SensorData>) (v1, v2) -> {
			long now = System.nanoTime();
			v1.setProcessingStartTime(now);
			return new SensorData(
					v1.getTimestamp(),
					v1.getEpoch(),
					v1.getMoteId(),
					(v1.getTemperature() + v2.getTemperature()) / 2,
					null, null, null);
		}).name("Window: Aggregation");

		DataStream<String> withLatency = aggregated
				.map(new EndToEndLatencyMapper())
				.name("Map[2]");

		FlinkKafkaProducer<String> kafkaSink = new FlinkKafkaProducer<>(outputTopic, new SimpleStringSchema(), kafkaProducerProps);
		withLatency.addSink(kafkaSink).name("Kafka Sink");

		env.execute("Sensor Assessment Job");
	}
}


class OperatorLatencyMapper extends RichMapFunction<SensorData, SensorData> {
	private transient Gauge<Long> latencyGauge;
	private final String name;

	public OperatorLatencyMapper(String name) {
		this.name = name;
	}

	@Override
	public void open(org.apache.flink.configuration.Configuration parameters) {
		latencyGauge = getRuntimeContext().getMetricGroup().gauge(name + "-latency", () -> 0L);
	}

	@Override
	public SensorData map(SensorData value) {
		long start = System.nanoTime();
		value.setProcessingStartTime(start); // Mark for end-to-end latency
		long end = System.nanoTime();
		long latency = (end - start) / 1_000_000;
		latencyGauge = getRuntimeContext().getMetricGroup().gauge(name + "-latency", () -> latency);
		return value;
	}
}

class EndToEndLatencyMapper extends RichMapFunction<SensorData, String> {
	private transient Gauge<Long> e2eLatency;

	@Override
	public void open(org.apache.flink.configuration.Configuration parameters) {
		e2eLatency = getRuntimeContext().getMetricGroup().gauge("end-to-end-latency", () -> 0L);
	}

	@Override
	public String map(SensorData value) {
		long now = System.nanoTime();
		long latency = (now - value.getProcessingStartTime()) / 1_000_000;
		e2eLatency = getRuntimeContext().getMetricGroup().gauge("end-to-end-latency", () -> latency);
		return String.format("MoteId: %d, AvgTemperature: %.4f, Latency: %dms",
				value.getMoteId(), value.getTemperature(), latency);
	}
}

