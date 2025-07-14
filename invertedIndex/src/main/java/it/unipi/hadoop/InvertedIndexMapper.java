package it.unipi.hadoop;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.*;
import java.util.*;

/*
    input:  (key, "il cane blu è giallo e blu")
    output: (("cane", file1:1) ("blu", file1:1), ("giallo", file1:1), ("blu", file1:1))
*/
public class InvertedIndexMapper extends Mapper<FileLineKey, Text, Text, CountPerFile> {
    // Constant IntWritable used to represent the count value "1"
    private static final IntWritable one = new IntWritable(1);
    private final Text token_key = new Text();
    private final Set<String> stopWords = new HashSet<>();

    // Loads the stopwords from the file "stopwords.txt" into a HashSet
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        String fileName = "stopwords.txt";
        try (InputStream input = getClass().getClassLoader().getResourceAsStream(fileName);
             BufferedReader reader = new BufferedReader(new InputStreamReader(input))) {

            if (input == null) {
                throw new IllegalArgumentException("File not found: " + fileName);
            }

            // Reads each line, cleans it, and adds to the stopWords set
            String line;
            while ((line = reader.readLine()) != null) {
                String cleaned = line.trim().toLowerCase();
                if (!cleaned.isEmpty()) {
                    stopWords.add(cleaned);
                }
            }
        } catch (IOException e) {
            // Increment a custom counter in case of error
            context.getCounter("Errors", "StopWordsSetup").increment(1);
            throw new IOException("Error while loading stop words", e);
        }
    }

    @Override
    public void map(final FileLineKey key, final Text value, final Context context) throws IOException, InterruptedException {
        String fileName = key.getFileName().toString();
        // Clean and filter the text
        String cleaned = preprocessing(value.toString());
        StringTokenizer itr = new StringTokenizer(cleaned);

        while (itr.hasMoreTokens()) {
            token_key.set(itr.nextToken());
            context.write(token_key, new CountPerFile(new Text(fileName), one));
        }
    }

    private String preprocessing(String text) {
        // Convert to lowercase, remove Saxon genitives and non-alphanumeric characters
        text = text.toLowerCase()
                .replaceAll("'s\\b", "")       // Removes Saxon genitives
                .replaceAll("[^a-z\\s]", " ")  // Removes non-alphanumeric characters
                .replaceAll("\\s+", " ")       // Remove multiple spaces
                .trim();

        // Stopwords filter
        StringBuilder processedWords = new StringBuilder();
        for (String token : text.split("\\s+")) {
            if (!stopWords.contains(token)) {
                processedWords.append(token).append(" ");
            }
        }
        return processedWords.toString().trim();
    }
}





