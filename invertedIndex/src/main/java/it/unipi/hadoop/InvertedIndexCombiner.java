package it.unipi.hadoop;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/*
    1 input: ((il, file1:1) (cane, file1:1) (il, file1:1) ... )
    more output: ((il, file1:2) (cane, file1:1) ... )
 */

public class InvertedIndexCombiner extends Reducer<Text, CountPerFile,Text,CountPerFile> {
    public void reduce(Text key, Iterable<CountPerFile> values, Context context) throws IOException, InterruptedException {
        List<String> result = new ArrayList<>();
        Map<String, Integer> fileCounts = new HashMap<>();

        for (CountPerFile val : values) {
            String fileName = val.getFileName().toString();
            int count = val.getCounter().get();

            // cerca nella map una chiave col nome di fileName, e somma il suo valore (oppure 0 se ancora non è presente) con count
            fileCounts.put(fileName, fileCounts.getOrDefault(fileName, 0) + count);
        }

        for (Map.Entry<String, Integer> entry : fileCounts.entrySet()) {
            context.write(key, new CountPerFile(new Text(entry.getKey()), new IntWritable(entry.getValue())));
        }
    }
}
