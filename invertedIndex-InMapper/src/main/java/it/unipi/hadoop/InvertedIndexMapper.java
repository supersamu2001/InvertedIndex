package it.unipi.hadoop;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.*;
import java.util.*;

/*
    1 input: file1
    more output: ((il, file1:1) (cane, file1:1) (il, file1:1) ... )
 */
public class InvertedIndexMapper extends Mapper<FileLineKey, Text, Text, Text> {
    private final Set<String> stopWords = new HashSet<>();
    private final Map<String, Integer> wordPerFiles = new HashMap<>();
    private String fileName;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        String fileName = "stopwords.txt";
        try (InputStream input = getClass().getClassLoader().getResourceAsStream(fileName);
             BufferedReader reader = new BufferedReader(new InputStreamReader(input))) {

            if (input == null) {
                throw new IllegalArgumentException("File not found: " + fileName);
            }

            String line;
            while ((line = reader.readLine()) != null) {
                String cleaned = line.trim().toLowerCase();
                if (!cleaned.isEmpty()) {
                    stopWords.add(cleaned);
                }
            }
        } catch (IOException e) {
            context.getCounter("Errors", "StopWordsSetup").increment(1);
            throw new IOException("Error while loading stop words", e);
        }
    }

    @Override
    protected void map(final FileLineKey key, final Text value, final Context context) throws IOException, InterruptedException {
        String actualFileName = key.getFileName().toString();
        String cleaned = preprocessing(value.toString());
        if(fileName == null) fileName = actualFileName;
        if(!fileName.equals(actualFileName)) {
            // the file change or is the first
            flush(context);
            fileName = actualFileName;
        }

        for (String token : cleaned.split("\\s+")) {
            if (token.isEmpty()) continue;
            wordPerFiles.merge(token, 1, Integer::sum);
        }

    }

    @Override
    protected void cleanup(Mapper<FileLineKey, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        // flush of the last file
        flush(context);
    }

    private void flush(Context context) throws IOException, InterruptedException {
        for (Map.Entry<String, Integer> entry : wordPerFiles.entrySet())
            context.write(new Text(entry.getKey()), new Text(fileName + ":" + entry.getValue()));
        wordPerFiles.clear();
    }
    
    private String preprocessing(String text) {
        // Converte in minuscolo, rimuove genitivi sassoni e caratteri non alfanumerici
        text = text.toLowerCase()
                .replaceAll("'s\\b", "")       // Rimuove genitivi sassoni
                .replaceAll("[^a-z\\s]", " ")  // Rimuove caratteri non alfanumerici
                .replaceAll("\\s+", " ")       // Riduce spazi multipli
                .trim();

        // Filtra le stop words
        StringBuilder processedWords = new StringBuilder();
        for (String token : text.split("\\s+")) {
            if (!stopWords.contains(token)) {
                processedWords.append(token).append(" ");
            }
        }
        return processedWords.toString().trim();
    }
}





