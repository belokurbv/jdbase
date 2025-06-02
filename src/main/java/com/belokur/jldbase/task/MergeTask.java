package com.belokur.jldbase.task;

import com.belokur.jldbase.api.KeyValueCodec;
import com.belokur.jldbase.api.Segment;
import com.belokur.jldbase.api.SegmentManager;
import com.belokur.jldbase.api.SegmentPosition;
import com.belokur.jldbase.impl.reader.DataReaderV1;
import com.belokur.jldbase.impl.writer.DataWriterV1;
import com.belokur.jldbase.index.SegmentKeyIndexManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

public class MergeTask implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(MergeTask.class);
    private final List<Segment> segments;
    private final SegmentManager segmentManager;
    private final SegmentKeyIndexManager indexManager;
    private final KeyValueCodec codec;
    private final int maxSegmentSize;
    private final Deque<Segment> tempSegments;

    public MergeTask(List<Segment> segments,
                     SegmentManager segmentManager,
                     SegmentKeyIndexManager indexManager,
                     KeyValueCodec codec,
                     int maxSegmentSize) {
        this.segments = segments;
        this.segmentManager = segmentManager;
        this.indexManager = indexManager;
        this.codec = codec;
        this.maxSegmentSize = maxSegmentSize;
        this.tempSegments = new ArrayDeque<>();
    }

    @Override
    public void run() {
        var frozenSnapshot = Map.copyOf(indexManager.getMemoryMap());
        indexManager.resetSnapshot();
        var newMap = mergeSegments(frozenSnapshot);
        indexManager.mergeSnapshot(newMap);
    }

    private DataWriterV1 createWriter() throws IOException {
        var tempSegment = segmentManager.createTemporarySegment();
        tempSegments.addLast(tempSegment);
        log.info("Creating temporary segment: {}", tempSegment.getId());
        return new DataWriterV1(tempSegment.getPath());
    }

    private Map<String, SegmentPosition> mergeSegments(Map<String, SegmentPosition> frozenSnapshot) {
        Map<String, SegmentPosition> newMap = new HashMap<>();

        for (var segment : segments) {
            if (!segment.isDirty()) {
                log.info("Segment {} is clean. Skipping merge.", segment.getId());
                continue;
            }

            try (var reader = new DataReaderV1(segment.getPath())) {
                var writer = createWriter();

                for (int keyIndex = segment.getKeys().nextSetBit(0);
                     keyIndex >= 0;
                     keyIndex = segment.getKeys().nextSetBit(keyIndex + 1)) {

                    var key = reader.readValue(reader.readSize());
                    var value = reader.readValue(reader.readSize());

                    var pos = frozenSnapshot.get(key);
                    if (pos != null && pos.segment().equals(segment)) {
                        var record = codec.toRecord(key, value);

                        if (writer.size() + record.length > maxSegmentSize) {
                            writer.close();
                            writer = createWriter();
                        }

                        var position = writer.size();

                        writer.append(record);

                        var tempSegment = this.tempSegments.peekLast();
                        newMap.put(key, new SegmentPosition(tempSegment, (int) position));
                        tempSegment.addKey((int) position);
                    }
                }

                writer.close();
            } catch (IOException e) {
                log.error("Failed to merge segment: {}", segment.getPath(), e);
                throw new RuntimeException("Failed to merge segment: " + segment.getPath(), e);
            }
        }


        if (!newMap.isEmpty()) {
            for (var tempSegment : tempSegments) {
                segmentManager.persist(tempSegment);
                segmentManager.deleteSegment(tempSegment);
            }
        } else {
            for (var tempSegment : tempSegments) {
                segmentManager.deleteTemporarySegment(tempSegment);
            }
        }

        return newMap;
    }
}
