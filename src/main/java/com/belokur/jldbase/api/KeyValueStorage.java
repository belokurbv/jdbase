package com.belokur.jldbase.api;

public interface KeyValueStorage {
    void set(String key, String value);

    String get(String key);
}
