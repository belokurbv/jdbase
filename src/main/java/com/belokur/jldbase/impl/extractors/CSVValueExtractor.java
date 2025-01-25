package com.belokur.jldbase.impl.extractors;

import com.belokur.jldbase.api.KeyValueExtractor;
import com.belokur.jldbase.api.Pair;

/**
 * In current example we assume that we use comma separated value storage
 */
public class CSVValueExtractor implements KeyValueExtractor {
    @Override
    public byte[] toRecord(String key, String value) {
        return (System.lineSeparator() + String.format("%s,%s", key, value)).getBytes();
    }

    @Override
    public Pair fromRecord(String row) {
        var array = row.split(",");
        return new Pair(array[0], array[1]);
    }

    @Override
    public boolean containsKey(String key, String content) {
        return content.split(",")[0].equals(key);
    }
}
