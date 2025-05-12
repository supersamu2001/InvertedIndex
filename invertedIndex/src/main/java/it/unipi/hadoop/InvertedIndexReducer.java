package it.unipi.hadoop;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.List;

/*
    input: (il, doc1:1  doc1:1, doc1:1, doc2:1, doc2:1)
    output: (il   doc1:3 doc2:2)
 */

public class InvertedIndexReducer extends Reducer<Text, CountPerFile,Text, List<CountPerFile>> {
    private IntWritable result = new IntWritable();
    public void reduce(Text key, Iterable<CountPerFile> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (CountPerFile val : values) {
            sum += val.getCounter().get();
        }
        result.set(sum);
        context.write(key, result);
    }
}
