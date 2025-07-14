package it.unipi.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class InvertedIndex {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        // Argument parsing
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        // At least 2 arguments are needed
        if (otherArgs.length < 2) {
            System.err.println("Usage: invertedIndex <in> <out>");
            System.exit(2);
        }

        // Job creation
        Job job = Job.getInstance(conf, "inverted index");
        job.setJarByClass(InvertedIndex.class);

        // Identify the classes to be called for each type of task
        job.setMapperClass(InvertedIndexMapper.class);
        job.setReducerClass(InvertedIndexReducer.class);

        // Identify the type of keys and values
        job.setInputFormatClass(MyInputFormat.class);
        // job.setMapOutputKeyClass(Text.class);    // not necessary
        // job.setMapOutputValueClass(Text.class);  // not necessary
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        // job.setNumReduceTasks(16);

        // Definition of the input split size
        CombineTextInputFormat.setMaxInputSplitSize(job, 1024 * 1024 * 128); // 128MB
        CombineFileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
