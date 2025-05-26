package com.belokur.jldbase;

import com.belokur.jldbase.api.KeyValueStorage;
import com.belokur.jldbase.segment.SegmentManagerImpl;
import com.belokur.jldbase.storage.LogStorageV4;
import com.belokur.jldbase.storage.SegmentsStorage;

import java.nio.file.Path;

public class LogStorageV4Test extends AbstractSegmentsStorageTest {
    @Override
    protected KeyValueStorage createStorage(Path path) {
        this.segmentManager = new SegmentManagerImpl(path);
        return new LogStorageV4(path.toString(), 50, segmentManager);
    }
}
