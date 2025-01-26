package com.belokur.jldbase;

import com.belokur.jldbase.api.KeyValueStorage;
import com.belokur.jldbase.v1.LogStorageV1;
import com.belokur.jldbase.v1.LogStorageV2;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;

class LogStorageV2Test extends AbstractKeyValueStorageTest {
    @Override
    protected KeyValueStorage createStorage(Path filePath) {
        return new LogStorageV2(filePath.toString());
    }
}
