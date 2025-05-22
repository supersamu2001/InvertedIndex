package it.unipi.hadoop;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;

public class CombineFileRecordReaderWrapper extends RecordReader<FileLineKey, Text> {

    private LineRecordReader lineReader = new LineRecordReader();
    private FileLineKey currentKey;
    private Text currentValue = new Text();

    private Path path;
    private long startOffset;

    public CombineFileRecordReaderWrapper(CombineFileSplit split, TaskAttemptContext context, Integer index) throws IOException {
        path = split.getPath(index);
        startOffset = split.getOffset(index);
        long length = split.getLength(index);
        FileSplit fileSplit = new FileSplit(path, startOffset, length, split.getLocations());
        lineReader.initialize(fileSplit, context);
    }

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException {
        // Already initialized in constructor
    }

    @Override
    public boolean nextKeyValue() throws IOException {
        if (!lineReader.nextKeyValue())
            return false;

        LongWritable offset = lineReader.getCurrentKey();    // offset riga
        Text line = lineReader.getCurrentValue();            // contenuto riga

        // crea la tua chiave personalizzata con nome file + offset
        currentKey = new FileLineKey(path.getName(), offset.get());
        currentValue = new Text(line);

        return true;
    }

    @Override
    public FileLineKey getCurrentKey() {
        return currentKey;
    }

    @Override
    public Text getCurrentValue() {
        return currentValue;
    }

    @Override
    public float getProgress() throws IOException {
        return lineReader.getProgress();
    }

    @Override
    public void close() throws IOException {
        lineReader.close();
    }
}

