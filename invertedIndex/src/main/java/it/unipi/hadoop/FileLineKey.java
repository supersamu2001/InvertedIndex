package it.unipi.hadoop;

import org.apache.hadoop.io.*;
import java.io.*;

public class FileLineKey implements WritableComparable<FileLineKey> {
    private Text fileName = new Text();
    private LongWritable offset = new LongWritable();

    // Default constructor
    public FileLineKey() {}

    // Constructor to create a FileLineKey with given fileName and offset
    public FileLineKey(String fileName, long offset) {
        this.fileName.set(fileName);
        this.offset.set(offset);
    }

    // Getter for fileName
    public Text getFileName() {
        return fileName;
    }

    // Getter for offset
    public LongWritable getOffset() {
        return offset;
    }

    // Serialize this object by writing fileName and offset to the DataOutput stream
    @Override
    public void write(DataOutput out) throws IOException {
        fileName.write(out);
        offset.write(out);
    }

    // Deserialize this object by reading fileName and offset from the DataInput stream
    @Override
    public void readFields(DataInput in) throws IOException {
        fileName.readFields(in);
        offset.readFields(in);
    }

    // Defines the ordering
    @Override
    public int compareTo(FileLineKey o) {
        int cmp = fileName.compareTo(o.fileName);
        if (cmp != 0) return cmp;
        return offset.compareTo(o.offset);
    }

    // Returns a string representation in the format: fileName:offset
    @Override
    public String toString() {
        return fileName.toString() + ":" + offset.get();
    }
}

