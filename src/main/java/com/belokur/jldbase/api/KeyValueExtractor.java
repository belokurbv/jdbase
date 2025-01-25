package com.belokur.jldbase.api;

public interface KeyValueExtractor {
    byte[] toRecord(String key, String value);

    Pair fromRecord(String row);

    boolean containsKey(String key, String content);
}
