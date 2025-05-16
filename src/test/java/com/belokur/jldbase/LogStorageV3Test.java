package com.belokur.jldbase;

import com.belokur.jldbase.api.KeyValueStorage;
import com.belokur.jldbase.v1.LogStorageV2;
import com.belokur.jldbase.v1.LogStorageV3;

import java.nio.file.Path;

class LogStorageV3Test extends AbstractKeyValueStorageTest {
    @Override
    protected KeyValueStorage createStorage(Path filePath) {
        return new LogStorageV3(filePath.toString());
    }
}
