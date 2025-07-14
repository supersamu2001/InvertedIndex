package it.unipi.hadoop;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/*
    input: (("blu", file1:2) ("blu", file2:1), ("blu", file3:1), ("blu", file4:4))
    output: ("blu", "file1:2 \t file2:1 \t file3:1 \t file4:4")
*/
public class InvertedIndexReducer extends Reducer<Text, CountPerFile, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<CountPerFile> values, Context context) throws IOException, InterruptedException {
        // List to save the final formatted result
        List<String> result = new ArrayList<>();
        // Map for total occurrences of the word for each file
        Map<String, Integer> fileCounts = new HashMap<>();

        for (CountPerFile val : values) {
            String fileName = val.getFileName().toString();
            int count = val.getCounter().get();
            // search the map for a key with the name fileName, and
            // add its value (or 0 if it is not present yet) with count
            fileCounts.put(fileName, fileCounts.getOrDefault(fileName, 0) + count);
        }

        for (Map.Entry<String, Integer> entry : fileCounts.entrySet()) {
            result.add(entry.getKey() + ":" + entry.getValue());
        }

        context.write(key, new Text(String.join("\t", result)));
    }
}
