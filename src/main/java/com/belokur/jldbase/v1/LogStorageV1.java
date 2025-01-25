package com.belokur.jldbase.v1;

import com.belokur.jldbase.api.KeyValueExtractor;
import com.belokur.jldbase.api.KeyValueStorage;
import com.belokur.jldbase.api.Pair;
import com.belokur.jldbase.exception.KeyException;
import com.belokur.jldbase.impl.extractors.CSVValueExtractor;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public class LogStorageV1 implements KeyValueStorage {
    private final Path path;

    private final KeyValueExtractor extractor;

    public LogStorageV1(String path) {
        this.path = Path.of(path);
        this.extractor = new CSVValueExtractor();
    }

    @Override
    public void set(String key, String value) {
        var content = extractor.toRecord(key, value);
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
                    .filter(line -> extractor.containsKey(key, line))
                    .reduce((first, second) -> second)
                    .map(extractor::fromRecord)
                    .map(Pair::value)
                    .orElseThrow(() -> new KeyException(key));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
