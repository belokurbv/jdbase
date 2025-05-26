package com.belokur.jldbase.storage;

import com.belokur.jldbase.api.KeyValueStorage;
import com.belokur.jldbase.api.Segment;
import com.belokur.jldbase.api.SegmentManager;
import com.belokur.jldbase.api.SegmentPosition;
import com.belokur.jldbase.exception.KeyException;
import com.belokur.jldbase.impl.codec.KeyValueBinaryCodec;
import com.belokur.jldbase.impl.reader.DataReaderV1;
import com.belokur.jldbase.impl.writer.DataWriterV1;

import java.io.IOException;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class LogStorageV4 extends SegmentsStorage implements KeyValueStorage {
    private static final int DEFAULT_SEGMENT_SIZE = 1024;

    private final ExecutorService mergeExecutor = Executors.newSingleThreadExecutor();

    public LogStorageV4(String path) {
        this(path, DEFAULT_SEGMENT_SIZE);
    }

    public LogStorageV4(String path, int segmentSize) {
        super(path, new KeyValueBinaryCodec());
        this.maxSegmentSize = segmentSize;
        Runtime.getRuntime().addShutdownHook(new Thread(mergeExecutor::shutdown));
    }

    public LogStorageV4(String path, int segmentSize, SegmentManager segmentManager) {
        super(path, new KeyValueBinaryCodec(), segmentManager);
        this.maxSegmentSize = segmentSize;
        Runtime.getRuntime().addShutdownHook(new Thread(mergeExecutor::shutdown));
    }

    public void init() {
        for (Segment segment : this.segmentManager.getAllSegments()) {
            loadKeysIntoMemory(segment);
        }
    }

    private void loadKeysIntoMemory(Segment segment) {
        try (var reader = new DataReaderV1(segment.getPath())) {
            while (reader.position() < reader.size()) {
                var recordStart = reader.position();
                var key = reader.readValue(reader.readSize());
                memoryMap.put(key, new SegmentPosition(segment, (int) recordStart));
                segment.addKey((int) recordStart);
                var valueSize = reader.readSize();
                reader.readValue(valueSize);
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to load keys from segment: " + segment.getPath(), e);
        }
    }

    public void merge(Segment segment) {
        if (!segment.isDirty()) {
            return;
        }

        this.frozenMap = memoryMap;
        this.memoryMap = new ConcurrentHashMap<>();
        var newMap = new HashMap<String, SegmentPosition>();

        var tempSegment = this.segmentManager.createTemporarySegment();

        try (var reader = new DataReaderV1(segment.getPath());
             var writer = new DataWriterV1(tempSegment.getPath())) {

            for (int keyIndex = segment.getKeys().nextSetBit(0);
                 keyIndex >= 0;
                 keyIndex = segment.getKeys().nextSetBit(keyIndex + 1)
            ) {
                var key = reader.readValue(reader.readSize());
                var value = reader.readValue(reader.readSize());

                if (isKeyStillInSegment(segment, key)) {
                    var record = codec.toRecord(key, value);
                    var position = writer.size();
                    writer.append(record);
                    newMap.put(key, new SegmentPosition(tempSegment, (int) position));
                }
            }

            retainOldKeys(newMap);
            memoryMap.putAll(newMap);
            segmentManager.persist(tempSegment);
        } catch (IOException e) {
            throw new RuntimeException("Failed to merge segment: " + segment.getPath(), e);
        } finally {
            cleanupAfterMerge(tempSegment);
        }
    }

    /**
     * During the merge process a new value can be created in the separate segment, so we have to manually check that our key
     * has not overwritten
     * @param segment current segment
     * @param key key we want to check
     * @return true if key has not been updated unless false
     */
    private boolean isKeyStillInSegment(Segment segment, String key) {
        var position = frozenMap.get(key);
        return position != null && position.segment().equals(segment);
    }


    private void retainOldKeys(Map<String, SegmentPosition> newMap) {
        for (var entry : frozenMap.entrySet()) {
            if (!newMap.containsKey(entry.getKey())) {
                newMap.put(entry.getKey(), entry.getValue());
            }
        }
    }

    private void cleanupAfterMerge(Segment tempSegment) {
        frozenMap = null;
        this.segmentManager.deleteTemporarySegment(tempSegment);
    }

    @Override
    public void set(String key, String value) {
        byte[] content = codec.toRecord(key, value);
        var current = this.segmentManager.getCurrent();
        try {
            long currentSize = Files.size(current.getPath());

            if (currentSize + content.length > maxSegmentSize) {
                var toMerge = current;
                mergeExecutor.submit(() -> this.merge(toMerge));
                var newSegment = segmentManager.addSegment();
                this.segmentManager.setCurrent(newSegment);
                current = newSegment;
            }

            set(current, key, content);
        } catch (IOException e) {
            throw new RuntimeException("Failed to write key-value pair to storage", e);
        }
    }

    private void set(Segment segment, String key, byte[] content) throws IOException {
        try (var writer = new DataWriterV1(segment.getPath())) {
            var currentSize = writer.size();
            writer.setPosition(currentSize);
            writer.append(content);
            var previousSegmentPosition = memoryMap.get(key);
            if (previousSegmentPosition != null) {
                segment.clearKey((int) previousSegmentPosition.position());
                segment.dirty();
            }
            memoryMap.put(key, new SegmentPosition(segment, (int) currentSize));
            segment.addKey((int) currentSize);
        }
    }

    @Override
    public String get(String key) {
        var position = getSegmentPosition(key);

        try (var reader = new DataReaderV1(position.segment().getPath())) {
            reader.setPosition(position.position());
            validateKeyInStorage(key, reader);

            int valueSize = reader.readSize();
            return reader.readValue(valueSize);
        } catch (IOException e) {
            throw new RuntimeException("Failed to read value for key: " + key, e);
        }
    }

    private SegmentPosition getSegmentPosition(String key) {
        SegmentPosition position = memoryMap.get(key);
        if (position == null) {
            position = frozenMap.get(key);
        }

        if (position == null) {
            throw new KeyException(key);
        }
        return position;
    }

    private void validateKeyInStorage(String key, DataReaderV1 reader) throws IOException {
        int keySize = reader.readSize();
        String storedKey = reader.readValue(keySize);

        if (!storedKey.equals(key)) {
            throw new KeyException("Key mismatch in storage: expected " + key + ", found " + storedKey);
        }
    }
}
