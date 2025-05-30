package com.belokur.jldbase.storage;

import com.belokur.jldbase.api.*;
import com.belokur.jldbase.segment.SegmentManagerV1;

import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public abstract class SegmentsStorage implements KeyValueStorage {
    ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
    protected KeyValueCodec codec;
    protected SegmentManager segmentManager;
    int maxSegmentSize = 100;


    public SegmentsStorage(String path, KeyValueCodec codec) {
        this.codec = codec;
        this.segmentManager = new SegmentManagerV1(Path.of(path));
        init();
    }

    public SegmentsStorage(KeyValueCodec codec, SegmentManager segmentManager) {
        this.codec = codec;
        this.segmentManager = segmentManager;
        init();
    }

    public abstract void merge(Segment segment);

    public SegmentManager getSegmentManager() {
        return segmentManager;
    }
}
