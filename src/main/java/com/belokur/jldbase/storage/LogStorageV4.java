package com.belokur.jldbase.storage;

import com.belokur.jldbase.api.KeyValueStorage;
import com.belokur.jldbase.api.Segment;
import com.belokur.jldbase.api.SegmentPosition;
import com.belokur.jldbase.exception.KeyException;
import com.belokur.jldbase.impl.codec.KeyValueBinaryCodec;
import com.belokur.jldbase.impl.reader.DataReaderV1;
import com.belokur.jldbase.impl.writer.DataWriterV1;

import java.io.IOException;
import java.nio.file.Files;
import java.util.BitSet;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class LogStorageV4 extends SegmentsStorage implements KeyValueStorage {
    private Map<String, SegmentPosition> memoryMap;

    public LogStorageV4(String path) {
        super(path, new KeyValueBinaryCodec());
    }

    public LogStorageV4(String path, int segmentSize) {
        super(path, new KeyValueBinaryCodec());
        this.maxSegmentSize = segmentSize;
    }

    public void init() {
        this.memoryMap = new ConcurrentHashMap<>();
        for (var segment : this.segments) {
            var path = segment.getPath();
            try (var reader = new DataReaderV1(path)) {
                while (reader.position() < reader.size()) {
                    var recordStart = reader.position();
                    var keyLen = reader.readSize();
                    var key = reader.readValue(keyLen);
                    var segmentPosition = new SegmentPosition(segment, recordStart);
                    memoryMap.put(key, segmentPosition);
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public void set(String key, String value) {
        var content = codec.toRecord(key, value);

        try {
            long currentSize = Files.size(current.getPath());

            if (currentSize + content.length > maxSegmentSize) {
                this.current = createSegment();
                currentSize = 0;
            }

            try (var writer = new DataWriterV1(current.getPath())) {
                writer.setPosition(currentSize);
                writer.append(content);
                var prevSegmentPosition = memoryMap.get(key);

                if (prevSegmentPosition != null) {
                    var position =  prevSegmentPosition.position();
                    current.clearKey((int) position);
                }

                var segmentPosition = new SegmentPosition(this.current, writer.size());
                memoryMap.put(key, segmentPosition);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void merge(Segment segment) {
        try (var reader = new DataReaderV1(segment.getPath())) {
            var seg = createSegment();
            var keys = segment.getKeys();
            for (int pos = keys.nextSetBit(0); pos >= 0; pos = keys.nextSetBit(pos + 1)) {
                var recordStart = reader.position();
                var keyLen = reader.readSize();
                var key = reader.readValue(keyLen);
                var segmentPosition = new SegmentPosition(segment, recordStart);
            }

            while (reader.position() < reader.size()) {
                int recordStart = (int) reader.position();

                if (!segment.keys.get(recordStart)) {
                    // key was deleted — skip
                    int keySize = reader.readSize();
                    reader.skip(keySize);
                    int valSize = reader.readSize();
                    reader.skip(valSize);
                    continue;
                }

                int keySize = reader.readSize();
                String key = reader.readValue(keySize);

                int valSize = reader.readSize();
                String value = reader.readValue(valSize);

                var currentSegment = memoryMap.get(key);
                if (currentSegment != null && currentSegment.segment() != segment) {
                    continue;
                }

                // rewrite key to new segment
                set(key, value);
                segment.clearKey(recordStart);
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to merge segment: " + segment.getPath(), e);
        }
    }

    @Override
    public String get(String key) {
        if (!memoryMap.containsKey(key)) {
            throw new KeyException(key);
        }

        var segmentPosition = memoryMap.get(key);
        var path = segmentPosition.segment().path();
        var position = segmentPosition.position();

        try (var reader = new DataReaderV1(path)) {
            reader.setPosition(position);
            var keySize = reader.readSize();
            var storedKey = reader.readValue(keySize);
            if (!storedKey.equals(key)) {
                throw new KeyException("Key does not match in the storage " + storedKey + " " + key);
            }
            var valSize = reader.readSize();
            return reader.readValue(valSize);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
