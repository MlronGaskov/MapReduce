package ru.nsu.mr.sorter;

import ru.nsu.mr.Deserializer;
import ru.nsu.mr.Pair;

import java.nio.file.Path;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

public class ExternalMergeSort {
    public static <KEY, VALUE> Iterator<Pair<KEY, VALUE>> merge(
        Comparator<KEY> comparator,
        Deserializer<KEY> keyDeserializer,
        Deserializer<VALUE> valueDeserializer,
        List<Path> filesToMerge
    ) {
        return new Iterator<>() {
            private final ReadersMinHeap<KEY, VALUE> heap = new ReadersMinHeap<>(
                filesToMerge,
                (a, b) -> comparator.compare(a.key(), b.key()),
                keyDeserializer,
                valueDeserializer
            );

            @Override
            public boolean hasNext() {
                return !heap.isHeapEmpty();
            }

            @Override
            public Pair<KEY, VALUE> next() {
                return heap.pollFromHeap();
            }
        };
    }
}
