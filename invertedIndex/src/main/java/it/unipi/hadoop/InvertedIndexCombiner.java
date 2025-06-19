package it.unipi.hadoop;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;


/*
    input: (("blu", file1:1) ("blu", file1:1), ("blu", file2:1), ("blu", file3:1))
    output: (("blu", file1:2) ("blu", file2:1), ("blu", file3:1))
*/
public class InvertedIndexCombiner extends Reducer<Text, CountPerFile, Text, CountPerFile> {

    @Override
    public void reduce(Text key, Iterable<CountPerFile> values, Context context) throws IOException, InterruptedException {
        Map<String, Integer> fileCounts = new HashMap<>();

        for (CountPerFile val : values) {
            String fileName = val.getFileName().toString();
            int count = val.getCounter().get();
            // search the map for a key with the name fileName, and add its value (or 0 if it is not present yet) with count
            fileCounts.put(fileName, fileCounts.getOrDefault(fileName, 0) + count);
        }

        for (Map.Entry<String, Integer> entry : fileCounts.entrySet()) {
            context.write(key, new CountPerFile(new Text(entry.getKey()), new IntWritable(entry.getValue())));
        }
    }
}
