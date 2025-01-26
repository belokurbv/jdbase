package com.belokur.jldbase.v1;

import com.belokur.jldbase.api.KeyValueExtractor;
import com.belokur.jldbase.api.KeyValueStorage;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public abstract class SingleFileStorage implements KeyValueStorage {
    protected Path path;

    protected KeyValueExtractor extractor;

    public SingleFileStorage(String path, String name, KeyValueExtractor extractor) {
        this.extractor = extractor;
        var entryPath = Path.of(path);
        if(Files.isDirectory(entryPath)) {
            var dbFile = entryPath.resolve(name);
            try {
                this.path = Files.createFile(dbFile);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } else {
            throw new RuntimeException("Path " + path + " is not a directory");
        }

    }
}
