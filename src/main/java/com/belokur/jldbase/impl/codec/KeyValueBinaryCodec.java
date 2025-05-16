package com.belokur.jldbase.impl.codec;

import com.belokur.jldbase.api.KeyValueCodec;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class KeyValueBinaryCodec implements KeyValueCodec {
    public static int CAPACITY = 4;

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
