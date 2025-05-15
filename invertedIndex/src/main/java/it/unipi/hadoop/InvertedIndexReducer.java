package it.unipi.hadoop;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/*
    input: (il ->  <file1,1> <file1,1> <file1,1> <file2,1> <file2,1> )
    output: (il -> <file1,3> <file2,2> )
 */

public class InvertedIndexReducer extends Reducer<Text, CountPerFile, Text, Text> {
    // private List<Text> result = new ArrayList<>();
    public void reduce(Text key, Iterable<CountPerFile> values, Context context) throws IOException, InterruptedException {
        List<Text> result = new ArrayList<>();
        int sum = 0;
        Text lastFilename = null;
        // Poiché l'ipotesi è che siano ordinati in ordine crescente per filename
        for (CountPerFile val : values) {
            // se siamo passati a un file diverso
            if(lastFilename != null && !lastFilename.equals(val.getFileName())) {
                CountPerFile singleFile = new CountPerFile(lastFilename, new IntWritable(sum));
                result.add(new Text(singleFile.toString()));
                sum = 0;
            }
            sum += val.getCounter().get();
            lastFilename = val.getFileName();
        }
        // inserisco l'ultimo file della lista
        result.add(new Text(new CountPerFile(lastFilename, new IntWritable(sum)).toString()));

        context.write(key, new Text(result.toString()));
    }
}
