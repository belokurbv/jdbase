package com.belokur.jldbase.impl.extractors;

import com.belokur.jldbase.api.KeyValueExtractor;

import java.nio.ByteBuffer;

public class BinaryPlainValueExtractor extends CSVValueExtractor implements KeyValueExtractor {
    public static int CAPACITY = 4;

    @Override
    public byte[] toRecord(String key, String value) {
        var content = "%s,%s".formatted(key, value);
        var data = content.getBytes();
        var buffer = ByteBuffer.allocate(CAPACITY + data.length);
        buffer.putInt(data.length);
        buffer.put(data);
        return buffer.array();
    }
}
