package com.belokur.jldbase.task;

import com.belokur.jldbase.api.KeyValueCodec;
import com.belokur.jldbase.api.Segment;
import com.belokur.jldbase.api.SegmentManager;
import com.belokur.jldbase.api.SegmentPosition;
import com.belokur.jldbase.impl.codec.KeyValueBinaryCodec;
import com.belokur.jldbase.impl.reader.DataReaderV1;
import com.belokur.jldbase.impl.writer.DataWriterV1;
import com.belokur.jldbase.index.SegmentKeyIndexManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class CompressTask implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(CompressTask.class);
    private final Segment segment;
    private final SegmentManager segmentManager;
    private final SegmentKeyIndexManager indexManager;
    private final KeyValueCodec codec;

    public CompressTask(Segment segment,
                        SegmentManager segmentManager,
                        SegmentKeyIndexManager indexManager,
                        KeyValueCodec codec) {
        this.segment = segment;
        this.segmentManager = segmentManager;
        this.indexManager = indexManager;
        this.codec = codec;
    }

    @Override
    public void run() {
        if (!segment.isDirty()) {
            log.info("Segment {} is clean. Skipping merge.", segment.getId());
            return;
        }

        log.info("Starting merge for segment {}", segment.getId());

        Map<String, SegmentPosition> frozenSnapshot = Map.copyOf(indexManager.getMemoryMap());
        indexManager.resetSnapshot();

        Map<String, SegmentPosition> newMap = mergeSegment(segment, frozenSnapshot);
        indexManager.mergeSnapshot(newMap);

        log.info("Merge completed for segment {}", segment.getId());
    }

    private Map<String, SegmentPosition> mergeSegment(Segment segment,
                                                      Map<String, SegmentPosition> frozenSnapshot) {
        Map<String, SegmentPosition> newMap = new HashMap<>();
        var tempSegment = segmentManager.createTemporarySegment();
        log.info("Creating temporary segment: {}", tempSegment.getId());

        try (var reader = new DataReaderV1(segment.getPath());
             var writer = new DataWriterV1(tempSegment.getPath())) {

            for (int keyIndex = segment.getKeys().nextSetBit(0);
                 keyIndex >= 0;
                 keyIndex = segment.getKeys().nextSetBit(keyIndex + 1)) {

                var key = reader.readValue(reader.readSize());
                var value = reader.readValue(reader.readSize());

                var pos = frozenSnapshot.get(key);
                if (pos != null && pos.segment().equals(segment)) {
                    var record = codec.toRecord(key, value);
                    var position = writer.size();
                    writer.append(record);
                    newMap.put(key, new SegmentPosition(tempSegment, (int) position));
                    tempSegment.addKey((int) position);
                }
            }
        } catch (IOException e) {
            log.error("Failed to merge segment: {}", segment.getPath(), e);
            throw new RuntimeException("Failed to merge segment: " + segment.getPath(), e);
        }

        if (!newMap.isEmpty()) {
            segmentManager.persist(tempSegment);
            segmentManager.deleteSegment(segment);
        } else {
            log.info("No keys were merged, deleting segment {}", segment.getId());
            segmentManager.deleteTemporarySegment(tempSegment);
        }

        return newMap;
    }
}
