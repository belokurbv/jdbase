package com.belokur.jldbase.storage;

import com.belokur.jldbase.api.KeyValueCodec;
import com.belokur.jldbase.api.KeyValueStorage;
import com.belokur.jldbase.api.Segment;
import com.belokur.jldbase.api.SegmentManager;
import com.belokur.jldbase.segment.SegmentManagerImpl;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

public abstract class SegmentsStorage implements KeyValueStorage {
    protected KeyValueCodec codec;
    protected Path root;
    protected SegmentManager segmentManager;
    int maxSegmentSize = 100;

    public SegmentsStorage(String path, KeyValueCodec codec) {
        this.codec = codec;
        this.segmentManager = new SegmentManagerImpl(Path.of(path));
        init();
    }

    public abstract void merge(Segment segment);
}
