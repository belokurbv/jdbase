package com.belokur.jldbase;

import com.belokur.jldbase.api.KeyValueStorage;
import com.belokur.jldbase.storage.LogStorageV4;
import com.belokur.jldbase.storage.SegmentsStorage;

import java.nio.file.Path;

public class LogStorageV5Test extends AbstractSegmentsStorageTest {
    @Override
    protected SegmentsStorage createStorage(Path path) {
        return new LogStorageV4(path.toString());
    }


}
