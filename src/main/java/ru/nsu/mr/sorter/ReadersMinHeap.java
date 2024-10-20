package ru.nsu.mr.sorter;

import ru.nsu.mr.Deserializer;
import ru.nsu.mr.Pair;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

public class ReadersMinHeap<KEY, VALUE> {
    private final PriorityQueue<Pair<Pair<KEY, VALUE>, Integer>> minHeap;
    private final List<BufferedReader> readers;
    private final Deserializer<KEY> keyDeserializer;
    private final Deserializer<VALUE> valueDeserializer;

    public ReadersMinHeap(
        List<Path> files,
        Comparator<Pair<KEY, VALUE>> comparator,
        Deserializer<KEY> keyDeserializer,
        Deserializer<VALUE> valueDeserializer
    ) {
        this.keyDeserializer = keyDeserializer;
        this.valueDeserializer = valueDeserializer;
        minHeap = new PriorityQueue<>((a, b) -> comparator.compare(a.key(), b.key()));
        readers = new ArrayList<>();
        try {
            for (int i = 0; i < files.size(); i++) {
                readers.add(Files.newBufferedReader(files.get(i)));
                String line = readers.get(i).readLine();
                if (line != null) {
                    String[] parts = line.split(" ", 2);
                    KEY key = keyDeserializer.deserialize(parts[0]);
                    VALUE value = valueDeserializer.deserialize(parts[1]);
                    minHeap.add(new Pair<>(new Pair<>(key, value), i));
                }
            }
        } catch (IOException e) {
            throw new RuntimeException();
        }
    }

    public boolean isHeapEmpty() {
        return minHeap.isEmpty();
    }

    public Pair<KEY, VALUE> pollFromHeap() {
        if (minHeap.isEmpty()) {
            return null;
        }
        Pair<Pair<KEY, VALUE>, Integer> minElement = minHeap.poll();
        Pair<KEY, VALUE> ans = minElement.key();
        try {
            if (readers.get(minElement.value()) != null) {
                String line = readers.get(minElement.value()).readLine();
                if (line == null) {
                    readers.get(minElement.value()).close();
                    readers.set(minElement.value(), null);
                } else {
                    String[] parts = line.split(" ", 2);
                    KEY key = keyDeserializer.deserialize(parts[0]);
                    VALUE value = valueDeserializer.deserialize(parts[1]);
                    minHeap.add(new Pair<>(new Pair<>(key, value), minElement.value()));
                }
            }
        } catch (IOException e) {
            throw new RuntimeException();
        }
        return ans;
    }
}
