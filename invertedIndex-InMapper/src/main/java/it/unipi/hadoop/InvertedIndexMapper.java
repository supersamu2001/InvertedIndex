package it.unipi.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.*;
import java.util.*;

/*
    1 input: file1
    more output: ((il, file1:1) (cane, file1:1) (il, file1:1) ... )
 */
public class InvertedIndexMapper extends Mapper<FileLineKey, Text, Text, CountPerFile> {
    private static final IntWritable one = new IntWritable(1);
    private final Text token_key = new Text();
    private final Set<String> stopWords = new HashSet<>();
    private Map<String, Map<String, Integer>> wordsPerFiles = new HashMap<>();

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
        String fileName = key.getFileName().toString();
        String cleaned = preprocessing(value.toString());
        StringTokenizer itr = new StringTokenizer(cleaned);

        while (itr.hasMoreTokens()) {
            String word = itr.nextToken();
            if(!wordsPerFiles.containsKey(word))
                wordsPerFiles.put(word, new HashMap<>());
            wordsPerFiles.get(word).merge(fileName, 1, Integer::sum);
        }

    }

    @Override
    protected void cleanup(Mapper<FileLineKey, Text, Text, CountPerFile>.Context context) throws IOException, InterruptedException {
        for(Map.Entry<String, Map<String, Integer>> entry : wordsPerFiles.entrySet()) {
            for(Map.Entry<String, Integer> sub_entry : entry.getValue().entrySet()) {
                CountPerFile countPerFile = new CountPerFile(new Text(sub_entry.getKey()), new IntWritable(sub_entry.getValue()));
                context.write(new Text(entry.getKey()), countPerFile);
            }
        }
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





