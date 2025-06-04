package com.belokur.jldbase.storage;

import com.belokur.jldbase.api.KeyValueStorage;
import com.belokur.jldbase.api.Segment;
import com.belokur.jldbase.api.SegmentManager;
import com.belokur.jldbase.api.SegmentPosition;
import com.belokur.jldbase.exception.KeyException;
import com.belokur.jldbase.impl.codec.KeyValueBinaryCodec;
import com.belokur.jldbase.impl.reader.DataReaderV1;
import com.belokur.jldbase.impl.writer.DataWriterV1;
import com.belokur.jldbase.segment.SegmentManagerV1;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class LogStorageV4 extends SegmentsStorage implements KeyValueStorage {
    private static final Logger log = LoggerFactory.getLogger(LogStorageV4.class);
    private final ExecutorService mergeExecutor = Executors.newSingleThreadExecutor();
    Map<String, SegmentPosition> frozenMap;
    Map<String, SegmentPosition> memoryMap;

    public LogStorageV4(String path) {
        this(new SegmentManagerV1(Path.of(path), SegmentManager.DEFAULT_SEGMENT_SIZE));
    }

    public LogStorageV4(String path, int segmentSize) {
        this(new SegmentManagerV1(Path.of(path), segmentSize));
    }

    public LogStorageV4(SegmentManager segmentManager) {
        super(new KeyValueBinaryCodec(), segmentManager);
        Runtime.getRuntime().addShutdownHook(new Thread(mergeExecutor::shutdown));
    }

    public void init() {
        rwLock.writeLock().lock();
        this.frozenMap = new ConcurrentHashMap<>();
        this.memoryMap = new ConcurrentHashMap<>();
        try {
            for (Segment segment : this.segmentManager.getAllSegments()) {
                loadKeysIntoMemory(segment);
            }
        } finally {
            rwLock.writeLock().unlock();
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
                log.info("Key {} loaded to segment {}", key, segment);
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to load keys from segment: " + segment.getPath(), e);
        }
    }

    public void merge(Segment segment) {
        if (!segment.isDirty()) return;

        log.info("Starting merge");

        rwLock.readLock().lock();
        try {
            frozenMap = new ConcurrentHashMap<>(memoryMap);
        } finally {
            rwLock.readLock().unlock();
        }

        var tempMap = new HashMap<String, SegmentPosition>();
        var tempSegment = segmentManager.createTemporarySegment();

        try (var reader = new DataReaderV1(segment.getPath());
             var writer = new DataWriterV1(tempSegment.getPath())) {

            for (int keyIndex = segment.getKeys().nextSetBit(0);
                 keyIndex >= 0;
                 keyIndex = segment.getKeys().nextSetBit(keyIndex + 1)) {

                var key = reader.readValue(reader.readSize());
                var value = reader.readValue(reader.readSize());

                if (memoryMap.containsKey(key)) {
                    log.info("Skipping key {} because it was updated during merge", key);
                    continue;
                }

                if (isKeyStillInSegment(segment, key)) {
                    var record = codec.toRecord(key, value);
                    var position = writer.size();
                    writer.append(record);
                    tempMap.put(key, new SegmentPosition(tempSegment, (int) position));
                    tempSegment.addKey((int) position);
                }
            }
        } catch (IOException e) {
            log.error("Failed to merge segment: {}", segment.getPath(), e);
            throw new RuntimeException("Failed to merge segment: " + segment.getPath(), e);
        }

        rwLock.writeLock().lock();
        try {
            if (!tempMap.isEmpty()) {
                var finalSegment = segmentManager.persist(tempSegment);
                var newMap = new HashMap<String, SegmentPosition>();
                for (var entry : tempMap.entrySet()) {
                    var pos = entry.getValue();
                    newMap.put(entry.getKey(), new SegmentPosition(finalSegment, pos.getPosition()));
                }
                retainOldKeys(newMap);
                memoryMap.putAll(newMap);
            } else{
                log.info("Segment {} is old, removing", segment.getId());
            }
            segmentManager.deleteSegment(segment);
        } finally {
            frozenMap = null;
            rwLock.writeLock().unlock();
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
        return position != null && position.getSegment().equals(segment);
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
        var maxSegmentSize = this.segmentManager.getMaxSegmentSize();
        log.info("Current segment {} {} {}", current, segmentManager.getAllSegments(), memoryMap);
        try {
            long currentSize = Files.size(current.getPath());

            if (currentSize + content.length > maxSegmentSize) {
                var toMerge = current;
                mergeExecutor.submit(() -> this.merge(toMerge));
                var newSegment = segmentManager.addSegment();
                this.segmentManager.setCurrent(newSegment);
                current = newSegment;
            }
            log.info("Current segment: {}", current);

            set(current, key, content);
        } catch (IOException e) {
            throw new RuntimeException("Failed to write key-value pair to storage", e);
        }
    }

    private void set(Segment segment, String key, byte[] content) throws IOException {
        rwLock.writeLock().lock();
        try (var writer = new DataWriterV1(segment.getPath())) {
            var currentSize = writer.size();
            writer.setPosition(currentSize);
            writer.append(content);
            var previousSegmentPosition = memoryMap.get(key);

            if (previousSegmentPosition != null) {
                segment.clearKey(previousSegmentPosition.getPosition());
                segment.dirty();
            }

            memoryMap.put(key, new SegmentPosition(segment, (int) currentSize));
            segment.addKey((int) currentSize);
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    @Override
    public String get(String key) {
        rwLock.readLock().lock();
        try {
            log.info(segmentManager.getAllSegments().toString());
            log.info(segmentManager.getCurrent().toString());
            log.info(getSegmentPosition(key).toString());

            var position = getSegmentPosition(key);

            try (var reader = new DataReaderV1(position.getSegment().getPath())) {
                reader.setPosition(position.getPosition());
                validateKeyInStorage(key, reader);

                int valueSize = reader.readSize();
                return reader.readValue(valueSize);
            } catch (IOException e) {
                throw new RuntimeException("Failed to read value for key: " + key, e);
            }
        } finally {
            rwLock.readLock().unlock();
        }

    }

    private SegmentPosition getSegmentPosition(String key) {
        rwLock.readLock().lock();
        try {
            var position = memoryMap.get(key);
            log.info(memoryMap.toString());
            if (position == null) {
                position = frozenMap.get(key);
            }

            if (position == null) {
                throw new KeyException(key);
            }
            return position;
        } finally {
            rwLock.readLock().unlock();
        }
    }

    private void validateKeyInStorage(String key, DataReaderV1 reader) throws IOException {
        int keySize = reader.readSize();
        String storedKey = reader.readValue(keySize);

        if (!storedKey.equals(key)) {
            throw new KeyException("Key mismatch in storage: expected " + key + ", found " + storedKey);
        }
    }
}
