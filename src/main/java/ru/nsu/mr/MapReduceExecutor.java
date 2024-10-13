package ru.nsu.mr;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

public interface MapReduceExecutor<KEY_IN, VALUE_IN, KEY_INTER, VALUE_INTER, KEY_OUT, VALUE_OUT> {
    void run(List<Path> inputFiles, Path mappersOutputDirectory, Path outputDirectory) throws IOException;
}
