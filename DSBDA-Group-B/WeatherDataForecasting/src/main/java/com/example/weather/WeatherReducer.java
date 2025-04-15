package com.example.weather;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

public class WeatherReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        double totalTemp     = 0;
        double totalHumidity = 0;
        double totalWind     = 0;
        int count            = 0;

        for (Text val : values) {
            String[] nums = val.toString().split(",");
            if (nums.length == 3) {
                try {
                    totalTemp     += Double.parseDouble(nums[0]);
                    totalHumidity += Double.parseDouble(nums[1]);
                    totalWind     += Double.parseDouble(nums[2]);
                    count++;
                } catch (NumberFormatException ignored) {}
            }
        }

        if (count > 0) {
            double avgTemp     = totalTemp / count;
            double avgHumidity = totalHumidity / count;
            double avgWind     = totalWind / count;

            context.write(new Text("Average Temperature (C):"), new Text(String.format("%.2f", avgTemp)));
            context.write(new Text("Average Humidity:"),      new Text(String.format("%.2f", avgHumidity)));
            context.write(new Text("Average Wind Speed (km/h):"), new Text(String.format("%.2f", avgWind)));
        }
    }
}
