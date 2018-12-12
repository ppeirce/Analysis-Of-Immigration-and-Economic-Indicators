import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AnalyticMapper 
extends Mapper<LongWritable, Text, Text, Text> {
    @Override
        public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
            String line = value.toString();
            String[] parts = line.split(",");
            if (parts.length >= 15) {
                String industryCode = parts[2].trim();
                String outputString = parts[1].trim() + "," + // county ID
                    parts[2].trim() + "," +  // industry code
                    parts[4].trim() + "," +  // county name
                    parts[5].trim() + "," +  // state name
                    parts[6].trim() + "," +  // 2010 # establishments
                    parts[7].trim() + "," +  // 2010 # employees
                    parts[8].trim() + "," +  // 2010 Q1 payroll (1k)
                    parts[9].trim() + "," +  // 2010 ann payroll (1k)
                    parts[11].trim() + "," + // 2014 # establishments
                    parts[12].trim() + "," + // 2014 # employees
                    parts[13].trim() + "," + // 2014 Q1 payroll (1k)
                    parts[14].trim() + "," + // 2014 ann payroll (1k)
                    parts[15].trim();        // % illegal imm 

                context.write(new Text(industryCode), new Text(outputString));
            }
        }
}
