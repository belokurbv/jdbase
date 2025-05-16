package com.belokur.jldbase.impl.extractors;

import com.belokur.jldbase.api.KeyValueExtractor;
import com.belokur.jldbase.api.Pair;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class BinaryValueExtractor extends CSVValueExtractor implements KeyValueExtractor {
    public static int CAPACITY = 4;

    public BinaryValueExtractor() {
    }

    @Override
    public Pair fromRecord(String row) {
        return super.fromRecord(row);
    }

    @Override
    public boolean containsKey(String key, String content) {
        return super.containsKey(key, content);
    }

    @Override
    public byte[] toRecord(String key, String value) {
        var keyBytes = key.getBytes(StandardCharsets.UTF_8);
        var valBytes = value.getBytes(StandardCharsets.UTF_8);
        var buffer = ByteBuffer.allocate(CAPACITY + keyBytes.length + CAPACITY + valBytes.length);
        buffer.putInt(keyBytes.length);
        buffer.put(keyBytes);
        buffer.putInt(valBytes.length);
        buffer.put(valBytes);
        return buffer.array();
    }
}
