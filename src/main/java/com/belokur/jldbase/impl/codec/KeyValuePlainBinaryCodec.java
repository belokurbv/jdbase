package com.belokur.jldbase.impl.codec;

import com.belokur.jldbase.api.KeyValueCodec;

import java.nio.ByteBuffer;

public class KeyValuePlainBinaryCodec implements KeyValueCodec {
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
