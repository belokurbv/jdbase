package com.belokur.jldbase.storage;

import com.belokur.jldbase.api.KeyValueStorage;
import com.belokur.jldbase.api.KeyValueCodec;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public abstract class SingleFileStorage implements KeyValueStorage {
    protected Path path;

    protected KeyValueCodec codec;

    public SingleFileStorage(String path, String name, KeyValueCodec codec) {
        this.codec = codec;
        var entryPath = Path.of(path);
        if (Files.isDirectory(entryPath)) {
            var dbFile = entryPath.resolve(name);
            if (!Files.exists(dbFile)) {
                try {
                    this.path = Files.createFile(dbFile);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            } else {
                this.path = dbFile;
            }
        } else {
            throw new RuntimeException("Path " + path + " is not a directory");
        }
        init();
    }
}
