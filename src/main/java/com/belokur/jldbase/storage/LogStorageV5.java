package com.belokur.jldbase.storage;

import com.belokur.jldbase.api.KeyValueStorage;
import com.belokur.jldbase.api.Segment;
import com.belokur.jldbase.api.SegmentManager;
import com.belokur.jldbase.api.SegmentPosition;
import com.belokur.jldbase.exception.KeyException;
import com.belokur.jldbase.impl.codec.KeyValueBinaryCodec;
import com.belokur.jldbase.impl.reader.DataReaderV1;
import com.belokur.jldbase.impl.writer.DataWriterV1;
import com.belokur.jldbase.index.SegmentKeyIndexManager;
import com.belokur.jldbase.index.SegmentKeyIndexManagerImplV1;
import com.belokur.jldbase.task.CompressTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class LogStorageV5 extends SegmentsStorage implements KeyValueStorage {
    private static final Logger log = LoggerFactory.getLogger(LogStorageV5.class);
    private static final int DEFAULT_SEGMENT_SIZE = 1024;
    private final ExecutorService mergeExecutor = Executors.newSingleThreadExecutor();
    private final SegmentKeyIndexManager indexManager;

    public LogStorageV5(String path) {
        this(path, DEFAULT_SEGMENT_SIZE);
    }

    public LogStorageV5(String path, int segmentSize) {
        super(path, new KeyValueBinaryCodec());
        this.maxSegmentSize = segmentSize;
        this.indexManager = new SegmentKeyIndexManagerImplV1();
        Runtime.getRuntime().addShutdownHook(new Thread(mergeExecutor::shutdown));
    }

    public LogStorageV5(String path, int segmentSize, SegmentManager segmentManager) {
        super(new KeyValueBinaryCodec(), segmentManager);
        this.maxSegmentSize = segmentSize;
        this.indexManager = new SegmentKeyIndexManagerImplV1();
        Runtime.getRuntime().addShutdownHook(new Thread(mergeExecutor::shutdown));
    }

    public void init() {
        rwLock.writeLock().lock();
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
                indexManager.put(key, new SegmentPosition(segment, (int) recordStart));
                segment.addKey((int) recordStart);
                var valueSize = reader.readSize();
                reader.readValue(valueSize);
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to load keys from segment: " + segment.getPath(), e);
        }
    }

    public void merge(Segment segment) {
        mergeExecutor.submit(new CompressTask(segment, segmentManager, indexManager, codec));
    }

    @Override
    public void set(String key, String value) {
        var content = codec.toRecord(key, value);
        var current = this.segmentManager.getCurrent();
        try {
            long currentSize = Files.size(current.getPath());

            if (currentSize + content.length > maxSegmentSize) {
                var toMerge = current;
                this.merge(toMerge);
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

            var previousSegmentPosition = indexManager.get(key);

            if (previousSegmentPosition != null) {
                segment.clearKey(previousSegmentPosition.position());
                segment.dirty();
            }

            indexManager.put(key, new SegmentPosition(segment, (int) currentSize));
            segment.addKey((int) currentSize);
        }
    }

    @Override
    public String get(String key) {
        rwLock.readLock().lock();
        try {
            var position = indexManager.get(key);

            try (var reader = new DataReaderV1(position.segment().getPath())) {
                reader.setPosition(position.position());
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

    private void validateKeyInStorage(String key, DataReaderV1 reader) throws IOException {
        int keySize = reader.readSize();
        String storedKey = reader.readValue(keySize);

        if (!storedKey.equals(key)) {
            throw new KeyException("Key mismatch in storage: expected " + key + ", found " + storedKey);
        }
    }
}
