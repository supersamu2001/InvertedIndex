package it.unipi.hadoop;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.*;
import java.util.*;

/*
    input:  (key, "il cane blu è giallo e blu")
    output: (("cane", file1:1) ("blu", file1:2), ("giallo", file1:1))
*/
public class InvertedIndexMapper extends Mapper<FileLineKey, Text, Text, Text> {
    private final Set<String> stopWords = new HashSet<>();
    private final Map<String, Integer> wordPerFiles = new HashMap<>();
    private String fileName;

    @Override
    protected void setup(Context context) throws IOException {
        // Loading stopwords file:
        String fileName = "stopwords.txt";
        try (InputStream input = getClass().getClassLoader().getResourceAsStream(fileName);
             BufferedReader reader = new BufferedReader(new InputStreamReader(input))) {

            String line;
            while ((line = reader.readLine()) != null) {
                // The words are made lowercase
                String cleaned = line.trim().toLowerCase();
                if (!cleaned.isEmpty()) {
                    // It adds the cleaned word to the stopwords file
                    stopWords.add(cleaned);
                }
            }
        } catch (IOException e) {
            //Exceptions if there are errors
            context.getCounter("Errors", "StopWordsSetup").increment(1);
            throw new IOException("Error while loading stop words", e);
        }
    }

    @Override
    protected void map(final FileLineKey key, final Text value, final Context context) throws IOException, InterruptedException {
        // It obtains the file name
        String actualFileName = key.getFileName().toString();
        // It cleans the text (making it lowercase and removing stopwords)
        String cleaned = preprocessing(value.toString());
        if(fileName == null) fileName = actualFileName;
        if(!fileName.equals(actualFileName)) {
            // the file change or is the first
            flush(context);
            fileName = actualFileName;
        }

        // Counter of words
        for (String token : cleaned.split("\\s+")) {
            if (token.isEmpty()) continue;

            // se la parola non è presente come chiave, allora metto 1, altrimenti aggiungo 1 al valore già presente
            wordPerFiles.merge(token, 1, Integer::sum);
        }

    }

    @Override
    protected void cleanup(Mapper<FileLineKey, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        // flush of the last file
        flush(context);
    }

    // Emission of the couple (word, file:counter) and flush
    private void flush(Context context) throws IOException, InterruptedException {
        for (Map.Entry<String, Integer> entry : wordPerFiles.entrySet())
            context.write(new Text(entry.getKey()), new Text(fileName + ":" + entry.getValue()));
        wordPerFiles.clear();
    }
    
    private String preprocessing(String text) {
        // Convertion to lowercase and sasson genitive and stopwords removal
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





