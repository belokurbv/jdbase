package com.belokur.jldbase.impl.extractors;

import com.belokur.jldbase.api.KeyValueExtractor;
import com.belokur.jldbase.api.KeyValueStorage;
import com.belokur.jldbase.api.Pair;

import java.nio.ByteBuffer;

public class BinaryValueExtractor implements KeyValueExtractor {

    @Override
    public byte[] toRecord(String key, String value) {
        var content = "%s,%s".formatted(key, value);
        var data = content.getBytes();
        ByteBuffer buffer = ByteBuffer.allocate(4 + data.length); // Создаём один буфер
        buffer.putInt(data.length);
        buffer.put(data);
        return buffer.array();
    }

    @Override
    public Pair fromRecord(String row) {
        return null;
    }

    @Override
    public boolean containsKey(String key, String content) {
        return false;
    }
}
