package it.unipi.hadoop;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/*
    1 input: (il ->  <file1,1> <file1,1> <file1,1> <file2,1> <file2,1> )
    1 output: (il -> <file1,3> <file2,2> )
 */

public class InvertedIndexReducer extends Reducer<Text, CountPerFile, Text, Text> {
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
            result.add(entry.getKey() + ":" + entry.getValue());
        }

        context.write(key, new Text(String.join("    ", result)));
    }
}
