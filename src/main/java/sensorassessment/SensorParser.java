package sensorassessment;

public class SensorParser {
    public static SensorData parse(String csvLine) {
        try {
            // Check if it is the head
            if (csvLine.startsWith("timestamp")) {
                return null; // skip
            }
            // Ensure minimum fields exist (timestamp, epoch, moteId)
            String[] parts = csvLine.split(",");
            if (parts.length < 3) return null; // Skip invalid rows
            Long timestamp = Long.parseLong(parts[0]);
            Integer epoch = Integer.parseInt(parts[1]);
            Integer moteId = Integer.parseInt(parts[2]);

            // Assign values if present, otherwise set to null
            Double temperature = parts.length > 3 && !parts[3].isEmpty() ? Double.parseDouble(parts[3]) : null;
            Double humidity = parts.length > 4 && !parts[4].isEmpty() ? Double.parseDouble(parts[4]) : null;
            Double light = parts.length > 5 && !parts[5].isEmpty() ? Double.parseDouble(parts[5]) : null;
            Double voltage = parts.length > 6 && !parts[6].isEmpty() ? Double.parseDouble(parts[6]) : null;

            return new SensorData(timestamp, epoch, moteId, temperature, humidity, light, voltage);
        } catch (Exception e) {
            throw new RuntimeException("Invalid data format: " + csvLine, e);
        }
    }
}
