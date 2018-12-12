import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Mapper2014
extends Mapper<LongWritable, Text, Text, Text> {
    @Override
        public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
            String line = value.toString();
            String[] parts = line.split(",");
            if (parts.length == 8) {
                String k = parts[0] + "," + parts[3];
                String v = "2014," +
                    parts[4] + "," +
                    parts[5] + "," +
                    parts[6] + "," +
                    parts[7];  
                if (!(parts[4].equals("") || parts[5].equals("") || parts[6].equals("") || parts[7].equals(""))) {
                    context.write(new Text(k), new Text(v));
                }
            }
        }
}
