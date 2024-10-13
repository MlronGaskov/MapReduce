package ru.nsu.mr;

import java.util.Iterator;
import java.util.function.Consumer;

public interface Reducer<KEY_IN, VALUE_IN, KEY_OUT, VALUE_OUT> {
    void reduce(KEY_IN key, Iterator<VALUE_IN> values, Consumer<Pair<KEY_OUT, VALUE_OUT>> output);
    Serializer<KEY_OUT> getKeyOutSerializer();
    Serializer<VALUE_OUT> getValueOutSerializer();
    Deserializer<KEY_IN> getKeyInDeserializer();
    Deserializer<VALUE_IN> getValueInDeserializer();
}
