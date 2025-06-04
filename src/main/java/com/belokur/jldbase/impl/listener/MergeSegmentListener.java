package com.belokur.jldbase.impl.listener;

import com.belokur.jldbase.api.KeyValueCodec;
import com.belokur.jldbase.api.Segment;
import com.belokur.jldbase.api.SegmentListener;
import com.belokur.jldbase.api.SegmentManager;
import com.belokur.jldbase.index.SegmentKeyIndexManager;
import com.belokur.jldbase.task.MergeTask;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class MergeSegmentListener implements SegmentListener {
    private final ExecutorService executor = Executors.newSingleThreadExecutor();
    private final SegmentManager segmentManager;
    private final SegmentKeyIndexManager indexManager;
    private final KeyValueCodec codec;

    public MergeSegmentListener(SegmentManager segmentManager,
                                KeyValueCodec codec,
                                SegmentKeyIndexManager indexManager) {
        this.segmentManager = segmentManager;
        this.codec = codec;
        this.indexManager = indexManager;
        Runtime.getRuntime().addShutdownHook(new Thread(executor::shutdown));
    }

    @Override
    public void onSegmentFull(Segment segment) {
        if (segmentManager.isCompactable()
        ) {
            executor.submit(
                    new MergeTask(
                            segmentManager.getOldSegments(),
                            segmentManager,
                            indexManager,
                            codec,
                            segmentManager.getMaxSegmentSize()
                    )
            );
        }
    }
}
