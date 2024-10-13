package ru.nsu.mr;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

public class MapReduceSequential<KEY_INTER, VALUE_INTER, KEY_OUT, VALUE_OUT>
    implements MapReduceExecutor<String, String, KEY_INTER, VALUE_INTER, KEY_OUT, VALUE_OUT>
{
    private final Mapper<String, String, KEY_INTER, VALUE_INTER> mapper;
    private final Reducer<KEY_INTER, VALUE_INTER, KEY_OUT, VALUE_OUT> reducer;
    private final int mappersCount;
    private final int reducersCount;

    public MapReduceSequential(
        Mapper<String, String, KEY_INTER, VALUE_INTER> mapper,
        Reducer<KEY_INTER, VALUE_INTER, KEY_OUT, VALUE_OUT> reducer,
        int mappersCount, int reducersCount
    ) {
        this.mapper = mapper;
        this.reducer = reducer;
        this.mappersCount = mappersCount;
        this.reducersCount = reducersCount;
    }

    private void reduceJob(List<Path> mappersOutputFiles, Path outputDirectory, int reducerId) throws IOException {
        PriorityQueue<QueueEntry> minHeap = new PriorityQueue<>();
        List<BufferedReader> readers = new ArrayList<>();

        try {
            for (Path file : mappersOutputFiles) {
                BufferedReader reader = Files.newBufferedReader(file);
                readers.add(reader);
                String line = reader.readLine();
                if (line != null) {
                    String key = line.split(" ", 2)[0];
                    minHeap.add(new QueueEntry(key, line, reader));
                }
            }

            Path outputFile = outputDirectory.resolve("output-" + reducerId + ".txt");
            BufferedWriter writer = Files.newBufferedWriter(outputFile);

            while (!minHeap.isEmpty()) {
                QueueEntry currentEntry = minHeap.peek();
                KEY_INTER currentKey = reducer.getKeyInDeserializer().deserialize(currentEntry.key);

                Iterator<VALUE_INTER> iterator = new Iterator<>() {
                    private QueueEntry currentEntry;
                    private String currentKey;

                    @Override
                    public boolean hasNext() {
                        if (currentEntry == null && !minHeap.isEmpty()) {
                            currentEntry = minHeap.peek();
                            currentKey = currentEntry.key;
                        } else if (!minHeap.isEmpty()) {
                            currentEntry = minHeap.peek();
                        }
                        return !minHeap.isEmpty() && currentEntry.key.equals(currentKey);
                    }

                    @Override
                    public VALUE_INTER next() {
                        if (minHeap.isEmpty() || !hasNext()) {
                            throw new NoSuchElementException();
                        }
                        currentEntry = minHeap.poll();
                        VALUE_INTER value = reducer.getValueInDeserializer().deserialize(
                            currentEntry.line.split(" ", 2)[1]
                        );
                        try {
                            String nextLine = currentEntry.reader.readLine();
                            if (nextLine != null) {
                                String key = nextLine.split(" ", 2)[0];
                                minHeap.add(new QueueEntry(key, nextLine, currentEntry.reader));
                            }
                        } catch (IOException e) {
                            throw new UncheckedIOException(e);
                        }
                        return value;
                    }
                };

                reducer.reduce(currentKey, iterator, output -> {
                    try {
                        String keySerialized = reducer.getKeyOutSerializer().serialize(output.key());
                        String valueSerialized = reducer.getValueOutSerializer().serialize(output.value());
                        writer.write(keySerialized + " " + valueSerialized);
                        writer.newLine();
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                });
            }

            writer.close();
            for (BufferedReader reader : readers) {
                reader.close();
            }

        } catch (IOException e) {
            throw new IOException("Error during reducing operation", e);
        }
    }

    private static class QueueEntry implements Comparable<QueueEntry> {
        String key;
        String line;
        BufferedReader reader;

        QueueEntry(String key, String line, BufferedReader reader) {
            this.key = key;
            this.line = line;
            this.reader = reader;
        }

        @Override
        public int compareTo(QueueEntry other) {
            return this.key.compareTo(other.key);
        }
    }

    private void mapperJob(List<Path> filesToMap, Path mappersOutputDirectory, int mapperId) throws IOException {
        List<BufferedWriter> writers = new ArrayList<>(reducersCount);
        for (int j = 0; j < reducersCount; ++j) {
            writers.add(Files.newBufferedWriter(mappersOutputDirectory.resolve(
                "mapper-output-" + mapperId + "-" + j + ".txt")));
        }

        for (Path inputFileToProcess: filesToMap) {
            BufferedReader reader = Files.newBufferedReader(inputFileToProcess);

            Iterator<Pair<String, String>> pairIterator = new Iterator<>() {
                String nextLine;
                boolean nextLineAvailable = false;

                @Override
                public boolean hasNext() {
                    if (!nextLineAvailable) {
                        try {
                            nextLine = reader.readLine();
                            nextLineAvailable = nextLine != null;
                        } catch (IOException e) {
                            throw new UncheckedIOException(e);
                        }
                    }
                    return nextLineAvailable;
                }

                @Override
                public Pair<String, String> next() {
                    if (!hasNext()) {
                        throw new NoSuchElementException();
                    }
                    String currentLine = nextLine;
                    nextLineAvailable = false;
                    return new Pair<>(inputFileToProcess.toString(), currentLine);
                }
            };

            mapper.map(pairIterator, output -> {
                int reducerIndex = determineReducerIndex(output.key());
                String keySerialized = mapper.getKeyOutSerializer().serialize(output.key());
                String valueSerialized = mapper.getValueOutSerializer().serialize(output.value());
                try {
                    writers.get(reducerIndex).write(keySerialized + " " + valueSerialized);
                    writers.get(reducerIndex).newLine();
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });
        }
        for (BufferedWriter writer: writers) {
            writer.close();
        }
        for (int i = 0; i < reducersCount; ++i) {
            sortFileByKey(mappersOutputDirectory.resolve("mapper-output-" + mapperId + "-" + i + ".txt"));
        }
    }

    private int determineReducerIndex(KEY_INTER key) {
        return Math.abs(mapper.keyOutHash(key)) % reducersCount;
    }

    private void sortFileByKey(Path fileToSort) throws IOException {
        List<Pair<String, String>> pairs = new ArrayList<>();

        try (BufferedReader reader = Files.newBufferedReader(fileToSort)) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split(" ", 2);
                if (parts.length == 2) {
                    String key = parts[0];
                    String value = parts[1];
                    pairs.add(new Pair<>(key, value));
                }
            }
        }
        pairs.sort(Comparator.comparing(Pair::key));
        try (BufferedWriter writer = Files.newBufferedWriter(fileToSort)) {
            for (Pair<String, String> pair : pairs) {
                writer.write(pair.key() + " " + pair.value());
                writer.newLine();
            }
        }
    }

    @Override
    public void run(List<Path> inputFiles, Path mappersOutputDirectory, Path outputDirectory) throws IOException {
        int numberOfProcessedInputFiles = 0;
        for (int i = 0; i < mappersCount; ++i) {
            int inputFilesToProcessCount = (inputFiles.size() - numberOfProcessedInputFiles) / (mappersCount - i);
            List<Path> inputFilesToProcess = new ArrayList<>();
            for (int k = 0; k < inputFilesToProcessCount; ++k) {
                Path inputFileToProcess = inputFiles.get(numberOfProcessedInputFiles + k);
                inputFilesToProcess.add(inputFileToProcess);
            }
            mapperJob(inputFilesToProcess, mappersOutputDirectory, i);
            numberOfProcessedInputFiles += inputFilesToProcessCount;
        }
        for (int i = 0; i < reducersCount; ++i) {
            List<Path> interFilesToReduce = new ArrayList<>();
            for (int k = 0; k < mappersCount; ++k) {
                interFilesToReduce.add(mappersOutputDirectory.resolve("mapper-output-" + k + "-" + i + ".txt"));
            }
            reduceJob(interFilesToReduce, outputDirectory, i);
        }
    }
}
