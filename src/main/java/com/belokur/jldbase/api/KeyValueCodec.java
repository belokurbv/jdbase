package com.belokur.jldbase.api;

public interface KeyValueCodec {
    byte[] toRecord(String key, String value);
}
