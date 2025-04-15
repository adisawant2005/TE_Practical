import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class LogLevelMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private Text logLevel = new Text();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Sample line: 2025-04-09 10:05:21 ERROR Unable to connect to database
        String line = value.toString();
        String[] parts = line.split(" ");
        if (parts.length >= 3) {
            logLevel.set(parts[2]); // Get the log level (INFO, ERROR, etc.)
            context.write(logLevel, one); // Emit (logLevel, 1)
        }
    }
}
