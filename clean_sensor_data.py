import pandas as pd
import re
from datetime import datetime

def clean_sensor_data(input_file, output_file):
    # Read the data
    data = []

    with open(input_file, 'r') as file:
        for line in file:
            line = line.strip()
            if not line:
                continue

            # Splitting lines using regular expressions
            parts = re.split(r'\s+', line)

            # Make sure if it has at least 3 parts
            if len(parts) < 3:
                continue

            # First three parts are date, time, and epoch
            date, time, epoch = parts[:3]
            remaining_values = parts[3:]

            #  Initialize variables
            moteid = temperature = humidity = light = voltage = None

            # Convert date and time to Unix timestamp
            try:
                timestamp = datetime.strptime(f"{date} {time}", "%Y-%m-%d %H:%M:%S.%f")
                unix_time = int(timestamp.timestamp() * 1e3)  # Convert to milliseconds (ms)
                
                # If there are more values, extract them without altering them
                if len(remaining_values) >= 1:
                    moteid = int(remaining_values[0]) if remaining_values[0].isdigit() else None
                if len(remaining_values) >= 2:
                    temperature = float(remaining_values[1]) if remaining_values[1] else None
                if len(remaining_values) >= 3:
                    humidity = float(remaining_values[2]) if remaining_values[2] else None
                if len(remaining_values) >= 4:
                    light = float(remaining_values[3]) if remaining_values[3] else None
                if len(remaining_values) >= 5:
                    voltage = float(remaining_values[4]) if remaining_values[4] else None
                    
                # Append all data, even if some values are missing
                data.append([unix_time, int(epoch), moteid, temperature, humidity, light, voltage])

            except ValueError:
                # Skip rows with invalid date or time formats
                continue
            

        # Create a DataFrame
        df = pd.DataFrame(data, columns=["timestamp", "epoch", "moteid", "temperature", "humidity", "light", "voltage"])
        
        # Drop rows with missing values
        df["moteid"] = df["moteid"].astype('Int64')

        # Write into the output file 
        df.to_csv(output_file, index=False, float_format="%.4f")
        print(f"Cleaned data saved to {output_file}")


# entry point
if __name__ == "__main__":
    input_file = "data.txt"  # input data
    output_file = "cleaned_data.csv"  # output data
    clean_sensor_data(input_file, output_file)
