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

public class InvertedIndexReducer extends Reducer<Text, Text, Text, Text> {
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        StringBuilder sb = new StringBuilder();
        boolean first = true;
        for (Text val : values) {
            if (!first) {
                sb.append("\t");
            }
            sb.append(val.toString());
            first = false;
        }
        context.write(key, new Text(sb.toString()));
    }
}
