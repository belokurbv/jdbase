package com.belokur.jldbase.storage;

import com.belokur.jldbase.api.*;
import com.belokur.jldbase.segment.SegmentManagerImpl;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public abstract class SegmentsStorage implements KeyValueStorage {
    protected KeyValueCodec codec;
    protected SegmentManager segmentManager;
    int maxSegmentSize = 100;
    Map<String, SegmentPosition> frozenMap;
    Map<String, SegmentPosition> memoryMap;

    public SegmentsStorage(String path, KeyValueCodec codec) {
        this.codec = codec;
        this.segmentManager = new SegmentManagerImpl(Path.of(path));
        this.frozenMap = new ConcurrentHashMap<>();
        this.memoryMap = new ConcurrentHashMap<>();
        init();
    }

    public SegmentsStorage(String path, KeyValueCodec codec, SegmentManager segmentManager) {
        this.codec = codec;
        this.segmentManager = segmentManager;
        this.frozenMap = new ConcurrentHashMap<>();
        this.memoryMap = new ConcurrentHashMap<>();
        init();
    }

    public abstract void merge(Segment segment);

    public SegmentManager getSegmentManager() {
        return segmentManager;
    }
}
