import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Mapper2010
extends Mapper<LongWritable, Text, Text, Text> {
    @Override
        public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
            String line = value.toString();
            String[] parts = line.split(",");
            if (parts.length == 8) {
                String k = parts[0] + "," + parts[3];
                String county = parts[1].substring(1); // drop the leading "
                String state  = parts[2].substring(1, parts[2].length() - 1); // drop leading space and trailing "
                String v = ",2010," +
                    county + "," +
                    state + "," +
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
