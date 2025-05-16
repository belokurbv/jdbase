package com.belokur.jldbase.storage;

import com.belokur.jldbase.api.KeyValueStorage;
import com.belokur.jldbase.api.Pair;
import com.belokur.jldbase.exception.KeyException;
import com.belokur.jldbase.impl.extractors.CSVValueCodec;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;

public class LogStorageV1 extends SingleFileStorage implements KeyValueStorage {
    public static final String DEFAULT_FILE_NAME = "db_v1.dat";

    public LogStorageV1(String path) {
        super(path, DEFAULT_FILE_NAME, new CSVValueCodec());
    }

    public static Pair fromRecord(String row) {
        var array = row.split(",");
        return new Pair(array[0], array[1]);
    }

    @Override
    public void set(String key, String value) {
        var content = codec.toRecord(key, value);
        try {
            Files.write(path, content, StandardOpenOption.APPEND);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String get(String key) {
        try (var lines = Files.lines(path)) {
            return lines
                    .filter(line -> containsKey(key, line))
                    .reduce((first, second) -> second)
                    .map(LogStorageV1::fromRecord)
                    .map(Pair::value)
                    .orElseThrow(() -> new KeyException(key));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public boolean containsKey(String key, String content) {
        return content.split(",")[0].equals(key);
    }
}
