package com.belokur.jldbase.impl.listener;

import com.belokur.jldbase.api.KeyValueCodec;
import com.belokur.jldbase.api.Segment;
import com.belokur.jldbase.api.SegmentListener;
import com.belokur.jldbase.api.SegmentManager;
import com.belokur.jldbase.index.SegmentKeyIndexManager;
import com.belokur.jldbase.task.CompressTask;
import com.belokur.jldbase.task.MergeTask;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class SegmentListenerV1 implements SegmentListener {
    public static final int MAX_SEGMENT_COUNT = 5;

    private final ExecutorService compactSegmentExecutor = Executors.newSingleThreadExecutor();

    private final ExecutorService mergeSegmentExecutor = Executors.newSingleThreadExecutor();

    private final SegmentManager segmentManager;

    private final KeyValueCodec codec;

    private final SegmentKeyIndexManager indexManager;

    public SegmentListenerV1(SegmentManager segmentManager, KeyValueCodec codec, SegmentKeyIndexManager indexManager) {
        this.segmentManager = segmentManager;
        this.codec = codec;
        this.indexManager = indexManager;
    }

    @Override
    public void onSegmentFull(Segment segment) {
        compactSegmentExecutor.submit(new CompressTask(segment, segmentManager, indexManager, codec));
        if (segmentManager.getAllSegments().size() == MAX_SEGMENT_COUNT) {
            mergeSegmentExecutor.submit(new MergeTask(segmentManager.getAllSegments(), segmentManager, indexManager, codec, segmentManager.getMaxSegmentSize()));
        }
    }
}
