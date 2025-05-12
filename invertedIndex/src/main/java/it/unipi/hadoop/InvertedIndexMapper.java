package it.unipi.hadoop;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.StringTokenizer;

/*
    input: il cane fa un salto
    output: (il, doc_name:1)  (cane, doc_name:1) ...
 */


public class InvertedIndexMapper extends Mapper<Object, Text, Text, CountPerFile> {
    private final static IntWritable one = new IntWritable(1);
    private final static Text fileName = new Text();
    private final Text token_key = new Text();
    private final CountPerFile countPerFile = new CountPerFile();

    public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
        // Ottieni lo split attuale, da cui puoi risalire al file
        FileSplit fileSplit = (FileSplit) context.getInputSplit();
        String fileName = fileSplit.getPath().getName();

        // suddivide la riga in token (parola singola)
        final StringTokenizer itr = new StringTokenizer(value.toString());

        while (itr.hasMoreTokens()) {
            countPerFile.setFileName(new Text(fileName));
            countPerFile.setCounter(one);

            token_key.set(itr.nextToken());
            context.write(token_key, countPerFile);
        }
    }
}
