package com.belokur.jldbase;

import com.belokur.jldbase.api.KeyValueStorage;
import com.belokur.jldbase.segment.SegmentManagerV1;
import com.belokur.jldbase.storage.LogStorageV4;

import java.nio.file.Path;

public class LogStorageV5Test extends AbstractSegmentsStorageTest {
    @Override
    protected KeyValueStorage createStorage(Path path) {
        this.segmentManager = new SegmentManagerV1(path);
        return new LogStorageV4( 50, segmentManager);
    }
}
