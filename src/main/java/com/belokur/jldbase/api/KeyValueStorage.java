package com.belokur.jldbase.api;

import java.util.List;
import java.util.Map;

public interface KeyValueStorage {
    void set(String key, String value);

    String get(String key);

    default void init() {};
}
