package it.unipi.hadoop;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CountPerFile implements WritableComparable<CountPerFile> {

    private Text fileName;
    private IntWritable counter;

    public CountPerFile() {
        this.fileName = new Text();
        this.counter = new IntWritable();
    }

    public CountPerFile(Text fileName, IntWritable counter) {
        this.fileName = fileName;
        this.counter = counter;
    }

    public Text getFileName() {
        return fileName;
    }

    public IntWritable getCounter() {
        return counter;
    }

    public void setFileName(Text fileName) {
        this.fileName = fileName;
    }

    public void setCounter(IntWritable counter) {
        this.counter = counter;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        fileName.write(out);
        counter.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        fileName.readFields(in);
        counter.readFields(in);
    }

    @Override
    public int compareTo(CountPerFile o) {
        // Ordina solo per nome del file (alfabetico crescente)
        return this.fileName.compareTo(o.fileName);
    }

    @Override
    public String toString() {
        return fileName.toString() + ":" + counter.get();
    }

    @Override
    public int hashCode() {
        return fileName.hashCode() * 163 + counter.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof CountPerFile) {
            CountPerFile other = (CountPerFile) o;
            return fileName.equals(other.fileName) && counter.equals(other.counter);
        }
        return false;
    }
}
