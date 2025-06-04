package com.belokur.jldbase.task;

import com.belokur.jldbase.api.KeyValueCodec;
import com.belokur.jldbase.api.Segment;
import com.belokur.jldbase.api.SegmentManager;
import com.belokur.jldbase.api.SegmentPositionVersioned;
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
    private final long snapshotVersion;

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
        this.snapshotVersion = indexManager.getVersion();
    }

    @Override
    public void run() {
        mergeSegments();
    }

    private DataWriterV1 createWriter() throws IOException {
        var tempSegment = segmentManager.createTemporarySegment();
        tempSegments.addLast(tempSegment);
        log.info("Creating temporary segment: {}", tempSegment.getId());
        return new DataWriterV1(tempSegment.getPath());
    }

    private void mergeSegments() {
        log.info("Merging segments: {}", segments.size());
        Map<String, SegmentPositionVersioned> tempMap = new HashMap<>();
        var dirtySegments = new LinkedList<Segment>();
        for (var segment : segments) {
            if (!segment.isDirty()) {
                log.info("Segment {} is clean. Skipping merge.", segment.getId());
                continue;
            }

            dirtySegments.add(segment);

            try (var reader = new DataReaderV1(segment.getPath())) {
                DataWriterV1 writer = createWriter();
                Map<String, SegmentPositionVersioned> segmentMap = new HashMap<>();
                Segment currentTempSegment = tempSegments.peekLast();

                for (int keyIndex = segment.getKeys().nextSetBit(0);
                     keyIndex >= 0;
                     keyIndex = segment.getKeys().nextSetBit(keyIndex + 1)) {

                    var key = reader.readValue(reader.readSize());
                    var value = reader.readValue(reader.readSize());

                    var pos = indexManager.get(key);

                    if (pos != null && pos.getVersion() > this.snapshotVersion) {
                        log.info("Skipping key {} because it was updated after snapshot", key);
                        continue;
                    }

                    if (pos != null && pos.getSegment().equals(segment)) {
                        var record = codec.toRecord(key, value);

                        if (writer.size() + record.length > this.maxSegmentSize) {
                            writer.close();
                            writer = createWriter();
                            currentTempSegment = tempSegments.peekLast();
                            segmentMap = new HashMap<>();
                        }

                        var position = writer.size();
                        writer.append(record);

                        segmentMap.put(key, new SegmentPositionVersioned(currentTempSegment, (int) position, pos.getVersion()));
                        currentTempSegment.addKey((int) position);
                    }
                }

                writer.close();

                // persist and update index
                if (!segmentMap.isEmpty()) {
                    var finalSegment = segmentManager.persist(currentTempSegment);
                    Map<String, SegmentPositionVersioned> newMap = new HashMap<>();
                    for (var entry : segmentMap.entrySet()) {
                        var pos = entry.getValue();
                        newMap.put(entry.getKey(),
                                   new SegmentPositionVersioned(finalSegment, pos.getPosition(), pos.getVersion()));
                    }
                    indexManager.putAll(newMap);
                }

                segmentManager.deleteTemporarySegment(currentTempSegment);

            } catch (IOException e) {
                log.error("Failed to merge segment: {}", segment.getPath(), e);
                throw new RuntimeException("Failed to merge segment: " + segment.getPath(), e);
            }
        }

        for (var segment : dirtySegments) {
            segmentManager.deleteSegment(segment);
        }

        log.info("after merge {}, ", indexManager.getMemoryMap());
        log.info("after merge {}, ", segmentManager.getAllSegments());
    }
}
