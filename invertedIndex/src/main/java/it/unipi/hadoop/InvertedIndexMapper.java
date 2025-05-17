package it.unipi.hadoop;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.StringTokenizer;

/*
    1 input: file1
    more output: ((il, file1:1) (cane, file1:1) (il, file1:1) ... )
 */

public class InvertedIndexMapper extends Mapper<Object, Text, Text, CountPerFile> {
    // private final static IntWritable one = new IntWritable(1);
    // private final Text token_key = new Text();

    public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
        // Ottieni lo split attuale, da cui puoi risalire al file
        FileSplit fileSplit = (FileSplit) context.getInputSplit();
        String fileName = fileSplit.getPath().getName();

        // suddivide il file in token (parola singola)
        final StringTokenizer itr = new StringTokenizer(value.toString());

        while (itr.hasMoreTokens()) {
            String word = itr.nextToken();
            CountPerFile countPerFile = new CountPerFile(new Text(fileName), new IntWritable(1));
            context.write(new Text(word), countPerFile);
        }

    }
}
