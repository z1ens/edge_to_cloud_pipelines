package sensorassessment;

public class SensorData {

    /**
     * Timestamp of the data in microseconds since epoch.
     */
    private Long timestamp;

    /**
     * Epoch is a monotonically increasing sequence number from each mote.
     */
    private Integer epoch;

    /**
     *  Sensor ID
     */
    private Integer moteId;

    /**
     * Temperature is in degrees Celsius (4 decimal places).
     */
    private Double temperature;

    /**
     * Humidity is temperature corrected relative humidity, ranging from 0-100% (4 decimal places).
     */
    private Double humidity;

    /**
     * Light is in Lux, a value of 1 Lux corresponds to moonlight,
     * 400 Lux to a bright office, and 100,000 Lux to full sunlight (4 decimal places).
     */
    private Double light;

    /**
     * Voltage is expressed in volts, ranging from 2-3;
     * the batteries, in this case, were lithium-ion cells that maintain a fairly constant voltage over their lifetime;
     * note that variations in voltage are highly correlated with temperature (4 decimal places).
     */
    private Double voltage;

    private transient long processingStartTime; // Use to calculate latency

    public SensorData(Long timestamp, Integer epoch, Integer moteId, Double temperature, Double humidity, Double light, Double voltage) {
        this.timestamp = timestamp;
        this.epoch = epoch;
        this.moteId = moteId;
        this.temperature = temperature;
        this.humidity = humidity;
        this.light = light;
        this.voltage = voltage;
        this.processingStartTime = System.nanoTime();
    }

    // Getters and setters
    public Long getTimestamp() { return timestamp; }
    public Integer getEpoch() { return epoch; }
    public Integer getMoteId() { return moteId; }
    public Double getTemperature() { return temperature; }
    public Double getHumidity() { return humidity; }
    public Double getLight() { return light; }
    public Double getVoltage() { return voltage; }

    public void setTemperature(Double temperature) { this.temperature = temperature; }
    public void setHumidity(Double humidity) { this.humidity = humidity; }
    public void setLight(Double light) { this.light = light; }
    public void setVoltage(Double voltage) { this.voltage = voltage; }

    public long getProcessingStartTime() { return processingStartTime; }
    public void setProcessingStartTime(long processingStartTime) { this.processingStartTime = processingStartTime; }

    @Override
    public String toString() {
        return "SensorData{" +
                "timestamp=" + timestamp +
                ", epoch=" + epoch +
                ", moteId=" + moteId +
                ", temperature=" + (temperature != null ? temperature : "null") +
                ", humidity=" + (humidity != null ? humidity : "null") +
                ", light=" + (light != null ? light : "null") +
                ", voltage=" + (voltage != null ? voltage : "null") +
                '}';
    }
}
