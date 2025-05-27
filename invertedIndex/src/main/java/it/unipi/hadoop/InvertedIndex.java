package it.unipi.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class InvertedIndex {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: invertedIndex <in> <out>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "inverted index");
        job.setJarByClass(InvertedIndex.class);
        job.setMapperClass(InvertedIndexMapper.class);
        job.setCombinerClass(InvertedIndexCombiner.class);
        job.setReducerClass(InvertedIndexReducer.class);
        job.setInputFormatClass(MyInputFormat.class);
        // job.setInputKeyClass(FileLineKey.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(CountPerFile.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // job.setInputFormatClass(CombineTextInputFormat.class); // Combine many small files
        CombineTextInputFormat.setMaxInputSplitSize(job, 1024 * 1024 * 32);

        /*
        for (int i = 0; i < otherArgs.length - 1; ++i)
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
         */

        CombineFileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
