package it.unipi.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;

/*
    1 input: file1
    more output: ((il, file1:1) (cane, file1:1) (il, file1:1) ... )
 */

public class InvertedIndexMapper extends Mapper<Object, Text, Text, CountPerFile> {
    private final static IntWritable one = new IntWritable(1);
    private final Text token_key = new Text();
    private final List<String> stop_words = new ArrayList<>();

    public void setup(Context context) {
        try {
            // Otteniamo la lista di stop words che andranno eliminate da ogni file, da parte dei relativi mapper.
            String fileName = "stopwords.txt";
            InputStream input = InvertedIndexMapper.class.getClassLoader().getResourceAsStream(fileName);
            if (input == null)
                throw new IllegalArgumentException("File not found: " + fileName);

            try (BufferedReader reader = new BufferedReader(new InputStreamReader(input))) {
                String linea;
                while ((linea = reader.readLine()) != null) {
                    if (!linea.trim().isEmpty()) {
                        stop_words.add(linea.trim());
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
        // Ottiene lo split attuale, da cui risalire al file
        FileSplit fileSplit = (FileSplit) context.getInputSplit();
        String fileName = fileSplit.getPath().getName();

        // rimuove parole e caratteri insignificanti
        String cleaned_words = preprocessing(value.toString());

        // suddivide il file in token (parola singola)
        final StringTokenizer itr = new StringTokenizer(cleaned_words);

        while(itr.hasMoreTokens()) {
            token_key.set(itr.nextToken());
            CountPerFile countPerFile = new CountPerFile(new Text(fileName), one);
            context.write(token_key, countPerFile);
        }

    }

    private String preprocessing(String text){
        // Ottenere il testo tutto minuscolo
        StringBuilder processed_words = new StringBuilder();
        String textLowerCase = text.toLowerCase();
        String[] tokens = textLowerCase.split("\\W+");                 // Divide il testo in singoli token
        for(String token : tokens){
            if(!token.isEmpty() && !stop_words.contains(token))
                processed_words.append(token).append(" ");
        }
        return processed_words.toString()
                .replaceAll("'s\\b", "")       // Rimuove genitivi sassoni
                .replaceAll("[^a-z\\s]", " ")  // Rimuove caratteri non alfanumerici
                .replaceAll("\\s+", " ")       // Riduce spazi multipli a uno singolo
                .trim();                                      // Rimuove spazi in cima e in fondo
    }
}
