package com.belokur.jldbase;

import com.belokur.jldbase.api.KeyValueStorage;
import com.belokur.jldbase.storage.LogStorageV2;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;

class LogStorageV2Test extends AbstractKeyValueStorageTest {
    @Override
    protected KeyValueStorage createStorage(Path filePath) {
        return new LogStorageV2(filePath.toString());
    }
}
