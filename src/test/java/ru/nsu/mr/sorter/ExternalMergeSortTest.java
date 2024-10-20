package ru.nsu.mr.sorter;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import ru.nsu.mr.Deserializer;
import ru.nsu.mr.Pair;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ExternalMergeSortTest {

    private static final List<Path> tempFiles = new ArrayList<>();
    private static final Deserializer<String> keyDeserializer = str -> str;
    private static final Deserializer<String> valueDeserializer = str -> str;

    @BeforeAll
    static void setUp() throws IOException {
        Path file1 = Files.createTempFile("sorted_file_1", ".txt");
        Path file2 = Files.createTempFile("sorted_file_2", ".txt");
        Path file3 = Files.createTempFile("sorted_file_3", ".txt");
        tempFiles.add(file1);
        tempFiles.add(file2);
        tempFiles.add(file3);

        try (BufferedWriter writer1 = Files.newBufferedWriter(file1)) {
            writer1.write("apple 1\n");
            writer1.write("apple 2\n");
            writer1.write("apple 3\n");
            writer1.write("banana 2\n");
            writer1.write("banana 2\n");
            writer1.write("banana 2\n");
            writer1.write("cherry 3\n");
        }

        try (BufferedWriter writer2 = Files.newBufferedWriter(file2)) {
            writer2.write("apricot 1\n");
            writer2.write("blueberry 2\n");
            writer2.write("date 3\n");
        }

        try (BufferedWriter writer3 = Files.newBufferedWriter(file3)) {
            writer3.write("a 1\n");
            writer3.write("y 2\n");
            writer3.write("z 3\n");
        }
    }

    @AfterAll
    static void tearDown() throws IOException {
        for (Path file : tempFiles) {
            Files.deleteIfExists(file);
        }
    }

    @Test
    void testMergeSortedFiles() {
        Iterator<Pair<String, String>> iterator = ExternalMergeSort.merge(
            Comparator.comparing(String::valueOf),
            keyDeserializer,
            valueDeserializer,
            tempFiles
        );

        List<Pair<String, String>> expectedResult = List.of(
            new Pair<>("a", "1"),
            new Pair<>("apple", "1"),
            new Pair<>("apple", "2"),
            new Pair<>("apple", "3"),
            new Pair<>("apricot", "1"),
            new Pair<>("banana", "2"),
            new Pair<>("banana", "2"),
            new Pair<>("banana", "2"),
            new Pair<>("blueberry", "2"),
            new Pair<>("cherry", "3"),
            new Pair<>("date", "3"),
            new Pair<>("y", "2"),
            new Pair<>("z", "3")
        );

        for (Pair<String, String> expected : expectedResult) {
            assertTrue(iterator.hasNext(), "Iterator has fewer elements than expected");
            Pair<String, String> actual = iterator.next();
            assertEquals(expected, actual, "Merged result does not match expected value");
        }

        assertFalse(iterator.hasNext(), "Iterator has unexpected additional elements");
    }
}
