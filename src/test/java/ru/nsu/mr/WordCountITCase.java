package ru.nsu.mr;

import org.junit.jupiter.api.Test;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class WordCountITCase {
    static class WordCounterMapper implements Mapper<String, String, String, Integer> {
        @Override
        public void map(Iterator<Pair<String, String>> input, Consumer<Pair<String, Integer>> output) {
            while (input.hasNext()) {
                Pair<String, String> split = input.next();
                for (String word : split.value().split("[\\s.,]+")) {
                    output.accept(new Pair<>(word.trim().toLowerCase(), 1));
                }
            }
        }

        @Override
        public Serializer<String> getKeyOutSerializer() {
            return input -> input;
        }

        @Override
        public Serializer<Integer> getValueOutSerializer() {
            return Object::toString;
        }

        @Override
        public int keyOutHash(String input) {
            return input.hashCode();
        }
    }

    static class WordCounterReducer implements Reducer<String, Integer, String, Integer> {
        @Override
        public void reduce(String key, Iterator<Integer> values, Consumer<Pair<String, Integer>> output) {
            int sum = 0;
            while (values.hasNext()) {
                sum += values.next();
            }
            output.accept(new Pair<>(key, sum));
        }

        @Override
        public Serializer<String> getKeyOutSerializer() {
            return input -> input;
        }

        @Override
        public Serializer<Integer> getValueOutSerializer() {
            return Object::toString;
        }

        @Override
        public Deserializer<String> getKeyInDeserializer() {
            return input -> input;
        }

        @Override
        public Deserializer<Integer> getValueInDeserializer() {
            return Integer::parseInt;
        }
    }

    public static ArrayList<String> generateWords(String[] words, int count) {
        ArrayList<String> result = new ArrayList<>();
        for (String someWord: words) {
            for (int i = 0; i < count; ++i) {
                result.add(someWord);
            }
        }
        Collections.shuffle(result);
        return result;
    }

    public static void readResult(String filename, Map<String, Integer> result) throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader(filename));
        String line;
        while ((line = reader.readLine()) != null) {
            String[] parts = line.split(" ");
            result.put(
                parts[0],
                result.getOrDefault(parts[0], 0) + Integer.parseInt(parts[1])
            );
        }
    }

    @Test
    public void testWordCounter() throws IOException {
        final int eachWordCount = 10;
        final String[] words = {"apple", "banana", "orange", "grape", "pear", "kiwi", "melon", "peach"};
        final int inputFilesCount = 10;

        final int mappersCount = 3;
        final int reducersCount = 4;
        List<Path> inputFiles = generatesInputFiles(inputFilesCount, words, eachWordCount);

        WordCounterMapper mapper = new WordCounterMapper();
        WordCounterReducer reducer = new WordCounterReducer();

        MapReduceExecutor<String, String, String, Integer, String, Integer> mr =
            new MapReduceSequential<>(mapper, reducer, mappersCount, reducersCount);

        Path outputPath = Files.createTempDirectory("outputs");
        Path mappersOutputPath = Files.createTempDirectory("mappers_outputs");
        mr.run(inputFiles, mappersOutputPath, outputPath);

        HashMap<String, Integer> mappersResult = new HashMap<>();
        for (int i = 0; i < mappersCount; ++i) {
            for (int j = 0; j < reducersCount; ++j) {
                readResult(mappersOutputPath.toString() + "/mapper-output-" + i + "-" + j + ".txt", mappersResult);
            }
        }
        for (String word: words) {
            int wordCntResult = mappersResult.get(word);
            assertEquals(inputFilesCount * eachWordCount, wordCntResult);
        }

        HashMap<String, Integer> reducesResult = new HashMap<>();
        for (int i = 0; i < reducersCount; ++i) {
            readResult(outputPath.toString() + "/output-" + i + ".txt", reducesResult);
        }
        for (String word: words) {
            int wordCntResult = reducesResult.get(word);
            assertEquals(inputFilesCount * eachWordCount, wordCntResult);
        }
    }

    private static List<Path> generatesInputFiles(int inputFilesCount, String[] words, int eachWordCount) throws IOException {
        List<Path> inputFiles = new ArrayList<>();

        for (int i = 1; i <= inputFilesCount; ++i) {
            Path tempFile = Files.createTempFile("TestFile" + i, ".txt");
            BufferedWriter writer = Files.newBufferedWriter(tempFile);
            writer.write(String.join(" ", generateWords(words, eachWordCount)));
            writer.close();

            inputFiles.add(tempFile);
        }
        return inputFiles;
    }
}