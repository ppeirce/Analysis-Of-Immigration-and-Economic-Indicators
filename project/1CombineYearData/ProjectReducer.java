import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ProjectReducer
            extends Reducer<Text, Text, Text, Text> {
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        String s = "";
        int count = 0;
        for (Text v : values) {
            String data = v.toString();
            if (data.substring(0,6).contains("10")) { // 2010
                s = data + s;
            } else { // 2014
                s = s + "," + data;
            }
            count += 1;
        }
        if (count == 2) { // ensure both years exist
            context.write(key, new Text(s));
        }
    }
}
