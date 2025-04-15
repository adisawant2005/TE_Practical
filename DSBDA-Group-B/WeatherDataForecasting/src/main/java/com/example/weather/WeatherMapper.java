package com.example.weather;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

public class WeatherMapper extends Mapper<LongWritable, Text, Text, Text> {
    private final static Text outKey = new Text("weather");

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString().trim();

        // Skip header line
        if (line.startsWith("Formatted Date") || line.contains("Temperature (C)")) {
            return;
        }

        // Split CSV on commas
        String[] parts = line.split(",");

        // Ensure we have at least 7 columns
        if (parts.length > 6) {
            try {
                String temperature = parts[3].trim();    // Temperature (C)
                String humidity    = parts[5].trim();    // Humidity
                String windSpeed   = parts[6].trim();    // Wind Speed (km/h)

                context.write(outKey, new Text(temperature + "," + humidity + "," + windSpeed));
            } catch (Exception e) {
                // Skip malformed rows
            }
        }
    }
}
