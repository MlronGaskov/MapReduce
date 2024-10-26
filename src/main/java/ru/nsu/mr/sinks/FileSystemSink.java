package ru.nsu.mr.sinks;

import java.io.IOException;

public interface FileSystemSink<K, V> {
    void put(K key, V value) throws IOException;
    void close() throws IOException;
}