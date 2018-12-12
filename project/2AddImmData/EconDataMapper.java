import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class EconDataMapper
extends Mapper<LongWritable, Text, Text, Text> {
    @Override
        public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
            // read data from the econ_data.csv file
            
            String line = value.toString();
            String[] parts = line.split(",");
            
            // grab the county and state names from the csv line and trim leading and trailing whitespace
            String county = parts[3].trim();
            String state  = parts[4].trim();

            // the key is the county and state names separated with a comma
            String k = county + "," + state;
            
            // add a leading 1 to the value to differentiate it from the imm_data.csv data
            line = "1," + line;
            context.write(new Text(k), new Text(line));
        }
}
