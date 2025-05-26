package com.belokur.jldbase.impl.codec;

import com.belokur.jldbase.api.KeyValueCodec;

import java.nio.ByteBuffer;

public class CSVCodec implements KeyValueCodec {
    @Override
    public byte[] toRecord(String key, String value) {
        var content = "%s,%s\n".formatted(key, value);
        return content.getBytes();
    }
}
