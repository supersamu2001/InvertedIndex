package it.unipi.hadoop;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/*
 * Custom WritableComparable class used to represent the occurrence of a word in a specific file.
 * It contains:
 *   - fileName (Text): the name of the file
 *   - counter (IntWritable): the number of times the word appears in that file
 */
public class CountPerFile implements WritableComparable<CountPerFile> {

    private Text fileName;
    private IntWritable counter;

    // Default constructor
    public CountPerFile() {
        this.fileName = new Text();
        this.counter = new IntWritable();
    }

    // Constructor with parameters
    public CountPerFile(Text fileName, IntWritable counter) {
        this.fileName = fileName;
        this.counter = counter;
    }

    // Getter for fileName
    public Text getFileName() {
        return fileName;
    }

    // Getter for counter
    public IntWritable getCounter() {
        return counter;
    }

    // Setter for fileName
    public void setFileName(Text fileName) {
        this.fileName = fileName;
    }

    // Setter for counter
    public void setCounter(IntWritable counter) {
        this.counter = counter;
    }

    // Serializes the object by writing its fields to a DataOutput stream
    @Override
    public void write(DataOutput out) throws IOException {
        fileName.write(out);
        counter.write(out);
    }

    // Deserializes the object by reading its fields from a DataInput stream
    @Override
    public void readFields(DataInput in) throws IOException {
        fileName.readFields(in);
        counter.readFields(in);
    }

    // Defines how two CountPerFile objects are compared
    @Override
    public int compareTo(CountPerFile o) {
        // Only alphabetically order
        return this.fileName.compareTo(o.fileName);
    }

    // Converts the object to a string in the format: fileName:count
    @Override
    public String toString() {
        return fileName.toString() + ":" + counter.get();
    }

    // Computes the hash code
    @Override
    public int hashCode() {
        return fileName.hashCode() * 163 + counter.hashCode();
    }

    // Defines equality between two CountPerFile objects
    @Override
    public boolean equals(Object o) {
        if (o instanceof CountPerFile) {
            CountPerFile other = (CountPerFile) o;
            return fileName.equals(other.fileName) && counter.equals(other.counter);
        }
        return false;
    }
}
