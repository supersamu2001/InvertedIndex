package it.unipi.hadoop;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/*
    input: ("blu", ((file1:2), (file2:1), (file3:4)))
    output: ("blu", "file1:2 \t file2:1 \t file3:4")
 */

// Reducer class: it takes a word (text) as key and a list of values (file:count)
public class InvertedIndexReducer extends Reducer<Text, Text, Text, Text> {
    // Reduce method called for each key
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // StringBuilder for creating output in string format
        StringBuilder sb = new StringBuilder();
        boolean first = true;
        // A tab is added at each element, but not at the first one
        for (Text val : values) {
            if (!first) {
                sb.append("\t");
            }
            // All the elements are added to the string
            sb.append(val.toString());
            first = false;
        }
        // It writes the result (word, list of file:counter) in the context
        context.write(key, new Text(sb.toString()));
    }
}
