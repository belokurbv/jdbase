package com.belokur.jldbase;

import com.belokur.jldbase.api.KeyValueStorage;
import com.belokur.jldbase.segment.SegmentManagerV1;
import com.belokur.jldbase.segment.SegmentManagerV2;
import com.belokur.jldbase.storage.LogStorageV4;
import com.belokur.jldbase.storage.LogStorageV5;

import java.nio.file.Path;

public class LogStorageV5Test extends AbstractSegmentsStorageTest {
    @Override
    protected KeyValueStorage createStorage(Path path) {
        this.segmentManager = new SegmentManagerV2(path, 50);
        return new LogStorageV5(segmentManager);
    }
}
