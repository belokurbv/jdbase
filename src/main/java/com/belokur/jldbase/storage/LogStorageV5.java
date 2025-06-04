package com.belokur.jldbase.storage;

import com.belokur.jldbase.api.KeyValueStorage;
import com.belokur.jldbase.api.Segment;
import com.belokur.jldbase.api.SegmentManager;
import com.belokur.jldbase.api.SegmentPositionVersioned;
import com.belokur.jldbase.exception.KeyException;
import com.belokur.jldbase.impl.codec.KeyValueBinaryCodec;
import com.belokur.jldbase.impl.listener.CompactSegmentListener;
import com.belokur.jldbase.impl.listener.MergeSegmentListener;
import com.belokur.jldbase.impl.reader.DataReaderV1;
import com.belokur.jldbase.impl.writer.DataWriterV1;
import com.belokur.jldbase.index.SegmentKeyIndexManager;
import com.belokur.jldbase.index.SegmentKeyIndexManagerImplV1;
import com.belokur.jldbase.segment.SegmentManagerV1;
import com.belokur.jldbase.segment.SegmentManagerV2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class LogStorageV5 extends SegmentsStorage implements KeyValueStorage {
    private static final Logger log = LoggerFactory.getLogger(LogStorageV5.class);

    private SegmentKeyIndexManager indexManager;

    public LogStorageV5(String path) {
        this(new SegmentManagerV2(Path.of(path), SegmentManager.DEFAULT_SEGMENT_SIZE));
    }

    public LogStorageV5(String path, int segmentSize) {
        this(new SegmentManagerV2(Path.of(path), segmentSize));
    }

    public LogStorageV5(SegmentManager segmentManager) {
        super(new KeyValueBinaryCodec(), segmentManager);
        segmentManager.addSegmentListener(new CompactSegmentListener(segmentManager, codec, indexManager));
        segmentManager.addSegmentListener(new MergeSegmentListener(segmentManager, codec, indexManager));
    }

    public void init() {
        this.indexManager = new SegmentKeyIndexManagerImplV1();
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
                indexManager.put(key, new SegmentPositionVersioned(segment, (int) recordStart, 0L));
                segment.addKey((int) recordStart);
                var valueSize = reader.readSize();
                reader.readValue(valueSize);
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to load keys from segment: " + segment.getPath(), e);
        }
    }

    @Override
    public void set(String key, String value) {
        var content = codec.toRecord(key, value);
        var current = this.segmentManager.getCurrent();
        var maxSegmentSize = this.segmentManager.getMaxSegmentSize();
        try {
            long currentSize = Files.size(current.getPath());

            if (currentSize + content.length > maxSegmentSize) {
                log.info("Segment {} is full", current.getId());
                segmentManager.notify(current);
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
                segment.clearKey(previousSegmentPosition.getPosition());
                segment.dirty();
            }

            indexManager.put(key, new SegmentPositionVersioned(segment, (int) currentSize,
                                                               indexManager.incrementAndGetVersion()));
            segment.addKey((int) currentSize);
        }
    }

    @Override
    public String get(String key) {
        rwLock.readLock().lock();
        try {
            var position = indexManager.get(key);

            if (position == null) {
                throw new KeyException("Key not found: " + key);
            }

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

    private void validateKeyInStorage(String key, DataReaderV1 reader) throws IOException {
        int keySize = reader.readSize();
        String storedKey = reader.readValue(keySize);

        if (!storedKey.equals(key)) {
            throw new KeyException("Key mismatch in storage: expected " + key + ", found " + storedKey);
        }
    }
}
