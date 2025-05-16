package com.belokur.jldbase;

import com.belokur.jldbase.api.KeyValueStorage;
import com.belokur.jldbase.storage.LogStorageV3;

import java.nio.file.Path;

class LogStorageV3Test extends AbstractKeyValueStorageTest {
    @Override
    protected KeyValueStorage createStorage(Path filePath) {
        return new LogStorageV3(filePath.toString());
    }
}
