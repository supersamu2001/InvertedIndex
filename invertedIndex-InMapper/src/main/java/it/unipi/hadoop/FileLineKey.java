package it.unipi.hadoop;

import org.apache.hadoop.io.*;
import java.io.*;

public class FileLineKey implements WritableComparable<FileLineKey> {
    private Text fileName = new Text();
    private LongWritable offset = new LongWritable();

    public FileLineKey() {}

    public FileLineKey(String fileName, long offset) {
        this.fileName.set(fileName);
        this.offset.set(offset);
    }

    public Text getFileName() {
        return fileName;
    }

    public LongWritable getOffset() {
        return offset;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        fileName.write(out);
        offset.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        fileName.readFields(in);
        offset.readFields(in);
    }

    @Override
    public int compareTo(FileLineKey o) {
        int cmp = fileName.compareTo(o.fileName);
        if (cmp != 0) return cmp;
        return offset.compareTo(o.offset);
    }

    @Override
    public String toString() {
        return fileName.toString() + ":" + offset.get();
    }
}

