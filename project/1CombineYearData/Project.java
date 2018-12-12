import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Project {
    
    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: WordCount <input path> <input path> <output path>");
            System.exit(-1);
        }

        Job job = new Job();
        job.setJarByClass(Project.class);
        job.setJobName("Project Data Cleaning");
        job.setNumReduceTasks(1);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, Mapper2010.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, Mapper2014.class);
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        job.setReducerClass(ProjectReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
