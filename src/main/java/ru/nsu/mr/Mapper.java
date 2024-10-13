package ru.nsu.mr;

import java.util.Iterator;
import java.util.function.Consumer;

public interface Mapper<KEY_IN, VALUE_IN, KEY_OUT, VALUE_OUT> {
    void map(Iterator<Pair<KEY_IN, VALUE_IN>> input, Consumer<Pair<KEY_OUT, VALUE_OUT>> output);
    Serializer<KEY_OUT> getKeyOutSerializer();
    Serializer<VALUE_OUT> getValueOutSerializer();
    int keyOutHash(KEY_OUT input);
}
