package ru.nsu.mr;

import ru.nsu.mr.config.Configuration;
import ru.nsu.mr.config.ConfigurationOption;
import ru.nsu.mr.sorter.ExternalMergeSort;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

public class MapReduceSequentialRunner<KEY_INTER, VALUE_INTER, KEY_OUT, VALUE_OUT>
    implements MapReduceRunner<KEY_INTER, VALUE_INTER, KEY_OUT, VALUE_OUT>
{
    private MapReduceJob<KEY_INTER, VALUE_INTER, KEY_OUT, VALUE_OUT> job;
    private Path outputDirectory;
    private Path mappersOutputPath;


    private int reducersCount;

    public MapReduceSequentialRunner() {
    }

    private void mapperJob(List<Path> filesToMap, int mapperId) {
        List<BufferedWriter> writers = new ArrayList<>(reducersCount);
        for (int j = 0; j < reducersCount; ++j) {
            try {
                writers.add(Files.newBufferedWriter(outputDirectory
                    .resolve(mappersOutputPath)
                    .resolve("mapper-output-" + mapperId + "-" + j + ".txt")));
            } catch (IOException e) {
                throw new RuntimeException();
            }
        }

        for (Path inputFileToProcess: filesToMap) {
            BufferedReader reader;
            String line;
            try {
                reader = Files.newBufferedReader(inputFileToProcess);
                line = reader.readLine();
            } catch (IOException e) {
                throw new RuntimeException();
            }

            Iterator<Pair<String, String>> iterator = new Iterator<>() {
                String nextLine = line;

                @Override
                public boolean hasNext() {
                    return nextLine != null;
                }

                @Override
                public Pair<String, String> next() {
                    if (!hasNext()) {
                        throw new RuntimeException();
                    }
                    String currentLine = nextLine;
                    try {
                        nextLine = reader.readLine();
                    } catch (IOException e) {
                        throw new RuntimeException();
                    }
                    return new Pair<>(inputFileToProcess.toString(), currentLine);
                }
            };

            job.getMapper().map(iterator, (outputKey, outputValue) -> {
                int reducerIndex = determineReducerIndex(outputKey);
                String keySerialized = job.getSerializerInterKey().serialize(outputKey);
                String valueSerialized = job.getSerializerInterValue().serialize(outputValue);
                try {
                    writers.get(reducerIndex).write(keySerialized + " " + valueSerialized);
                    writers.get(reducerIndex).newLine();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
        }
        try {
            for (BufferedWriter writer: writers) {
                writer.close();
            }
        } catch (IOException e) {
            throw new RuntimeException();
        }
        for (int i = 0; i < reducersCount; ++i) {
            sortFileByKey(mappersOutputPath.resolve("mapper-output-" + mapperId + "-" + i + ".txt"));
        }
    }

    private void sortFileByKey(Path fileToSort){
        List<Pair<String, String>> pairs = new ArrayList<>();

        try (BufferedReader reader = Files.newBufferedReader(fileToSort)) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split(" ", 2);
                String key = parts[0];
                String value = parts[1];
                pairs.add(new Pair<>(key, value));
            }
        } catch (IOException e) {
            throw new RuntimeException();
        }

        pairs.sort(Comparator.comparing(Pair::value));

        try (BufferedWriter writer = Files.newBufferedWriter(fileToSort)) {
            for (Pair<String, String> pair : pairs) {
                writer.write(pair.key() + " " + pair.value());
                writer.newLine();
            }
        } catch (IOException e) {
            throw new RuntimeException();
        }
    }

    private int determineReducerIndex(KEY_INTER key) {
        return Math.abs(job.getHasher().hash(key)) % reducersCount;
    }

    @SuppressWarnings("unchecked")
    private void reduceJob(List<Path> mappersOutputFiles, int reducerId) {
        Iterator<Pair<KEY_INTER, VALUE_INTER>> iteratorKeyValue = ExternalMergeSort.merge(
            job.getComparator(),
            job.getDeserializerInterKey(),
            job.getDeserializerInterValue(),
            mappersOutputFiles
        );
        Path outputFile = outputDirectory.resolve("output-" + reducerId + ".txt");
        if (!iteratorKeyValue.hasNext()) {
            return;
        }

        BufferedWriter writer;
        try {
            writer = Files.newBufferedWriter(outputFile);
        } catch (IOException e) {
            throw new RuntimeException();
        }


        final Pair<KEY_INTER, VALUE_INTER>[] currentKeyValue = new Pair[]{iteratorKeyValue.next()};
        while (currentKeyValue[0] != null) {
            KEY_INTER currentKey = currentKeyValue[0].key();
            Iterator<VALUE_INTER> iterator = new Iterator<VALUE_INTER>() {
                boolean isKeyChanged = false;
                @Override
                public boolean hasNext() {
                    return currentKeyValue[0] != null && !isKeyChanged;
                }

                @Override
                public VALUE_INTER next() {
                    Pair<KEY_INTER, VALUE_INTER> prev = currentKeyValue[0];
                    currentKeyValue[0] = iteratorKeyValue.next();
                    if (currentKeyValue[0] == null || currentKeyValue[0].key() != prev.key()) {
                        isKeyChanged = true;
                    }
                    return prev.value();
                }
            };
            job.getReducer().reduce(currentKey, iterator, (outputKey, outputValue) -> {
                try {
                    String keySerialized = job.getSerializerOutKey().serialize(outputKey);
                    String valueSerialized = job.getSerializerOutValue().serialize(outputValue);
                    writer.write(keySerialized + " " + valueSerialized);
                    writer.newLine();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
        }
        try {
            writer.close();
        } catch (IOException e) {
            throw new RuntimeException();
        }
    }

    @Override
    public void run(
        MapReduceJob<KEY_INTER, VALUE_INTER, KEY_OUT, VALUE_OUT> job,
        List<Path> inputFiles,
        Configuration configuration,
        Path mappersOutputDirectory,
        Path outputDirectory)
    {
        this.job = job;
        this.outputDirectory = outputDirectory;
        this.mappersOutputPath = mappersOutputDirectory;

        int mappersCount = configuration.get(ConfigurationOption.MAPPERS_COUNT);
        reducersCount = configuration.get(ConfigurationOption.REDUCERS_COUNT);

        int numberOfProcessedInputFiles = 0;
        for (int i = 0; i < mappersCount; ++i) {
            int inputFilesToProcessCount = (inputFiles.size() - numberOfProcessedInputFiles) / (mappersCount - i);
            List<Path> inputFilesToProcess = new ArrayList<>();
            for (int k = 0; k < inputFilesToProcessCount; ++k) {
                Path inputFileToProcess = inputFiles.get(numberOfProcessedInputFiles + k);
                inputFilesToProcess.add(inputFileToProcess);
            }
            mapperJob(inputFilesToProcess, i);
            numberOfProcessedInputFiles += inputFilesToProcessCount;
        }
        for (int i = 0; i < reducersCount; ++i) {
            List<Path> interFilesToReduce = new ArrayList<>();
            for (int k = 0; k < mappersCount; ++k) {
                interFilesToReduce.add(mappersOutputPath.resolve("mapper-output-" + k + "-" + i + ".txt"));
            }
            reduceJob(interFilesToReduce, i);
        }
    }
}
