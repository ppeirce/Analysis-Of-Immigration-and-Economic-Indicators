import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CombineReducer
extends Reducer<Text, Text, Text, Text> {
    @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {
            String percent = "";
            ArrayList<Text> cache = new ArrayList<Text>();

            // first loop: look for the imm_data value and pull the percentage from it
            for (Text v : values) {
                Text t = new Text();
                t.set(v);
                cache.add(t);

                String s = v.toString();
                if (s.charAt(0) == '2') { // this value is the imm_data
                    String[] parts = s.split(",");
                    percent = parts[3]; // save the percent 
                }
            }

            int size = cache.size();

            // second loop: append percentage to econ_data values and write them to context
            //for (Text v : values) {
            for (int i = 0; i < size; ++i) {
                //String s = v.toString();
                String s = cache.get(i).toString();
                if (s.charAt(0) == '1' && !percent.isEmpty()) { // so we don't write the immigration data row 
                    String output = s + "," + percent; // append percent
                    context.write(new Text(""), new Text(output)); // write the value 
                }
            }
        }
}
