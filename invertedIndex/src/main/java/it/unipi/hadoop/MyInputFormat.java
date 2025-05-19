package it.unipi.hadoop;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineFileRecordReader;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.io.*;

import java.io.IOException;

public class MyInputFormat extends CombineFileInputFormat<FileLineKey, Text> {

    @Override
    public RecordReader<FileLineKey, Text> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException {
        return new CombineFileRecordReader<FileLineKey, Text>((CombineFileSplit) split, context, CombineFileRecordReaderWrapper.class);
    }
}

