package com.belokur.jldbase;

import com.belokur.jldbase.api.KeyValueStorage;
import com.belokur.jldbase.storage.LogStorageV1;

import java.nio.file.Path;

class LogStorageV1Test extends AbstractKeyValueStorageTest {
    @Override
    protected KeyValueStorage createStorage(Path filePath) {
        return new LogStorageV1(filePath.toString());
    }
}
