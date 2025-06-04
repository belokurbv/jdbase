package com.belokur.jldbase.segment;

import com.belokur.jldbase.api.Segment;
import com.belokur.jldbase.api.SegmentListener;
import com.belokur.jldbase.api.SegmentManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

public class SegmentManagerV2 implements SegmentManager {
    private static final Logger log = LoggerFactory.getLogger(SegmentManagerV2.class);
    public static final int MAX_SEGMENT_COUNT = 5;
    public static final int OLD_SEGMENTS_COUNT = 2;
    public static final String EXTENSION = ".dat";
    public static final String TEMPORARY_SEGMENT_EXTENSION = ".tmp";
    public static final String PREFIX = "segment";
    public static final String SEGMENT_TEMPLATE = "%s-%08d%s";
    private final ReentrantLock lock = new ReentrantLock();
    private final List<SegmentListener> listeners ;
    private final AtomicInteger maxSegmentId;
    private final List<Segment> segments;
    private final Path root;
    private final int segmentSize;
    private volatile Segment current;

    public SegmentManagerV2(Path root) {
        this(root, DEFAULT_SEGMENT_SIZE);
    }

    public SegmentManagerV2(Path root, int segmentSize) {
        this.root = root;
        this.segments = new CopyOnWriteArrayList<>();
        this.maxSegmentId = new AtomicInteger();
        this.segmentSize = segmentSize;
        this.listeners = new CopyOnWriteArrayList<>();
        initSegments(root);
    }

    public static int getSegmentId(Path segment) {
        var segmentId = segment.getFileName()
                .toString()
                .replace(PREFIX + "-", "")
                .replace(EXTENSION, "");

        return Integer.parseInt(segmentId);
    }

    @Override
    public void initSegments(Path root) {
        if (!Files.isDirectory(root)) {
            throw new IllegalArgumentException("Path " + root + " should be a valid directory");
        }

        try (var files = Files.list(root)) {
            var fileSegments = files
                    .peek(file -> log.info("File loaded {}", file))
                    .filter(p -> p.toString().endsWith(EXTENSION))
                    .map(file -> new Segment(getSegmentId(file), file))
                    .toList();
            this.segments.addAll(fileSegments);
            log.info("Loaded {} segments", segments.size());
            var currMax = segments
                    .stream()
                    .map(Segment::getId)
                    .mapToInt(Integer::intValue)
                    .max()
                    .orElse(0);
            maxSegmentId.set(currMax);
            log.info("Current max {}", currMax);
            log.info("MaxSegmentId {}", maxSegmentId.get());
        } catch (IOException e) {
            throw new RuntimeException("Can't get segments from folder" + e);
        }

        this.current = addSegment();
    }

    @Override
    public Segment addSegment() {
        var id = maxSegmentId.incrementAndGet();
        var segment = createEmptySegment(id, EXTENSION);
        segments.add(segment);
        return segment;
    }

    private Segment createEmptySegment(int segmentId, String extension) {
        var newSegmentName = String.format(SEGMENT_TEMPLATE, PREFIX, segmentId, extension);
        var newSegmentPath = root.resolve(newSegmentName);
        try {
            var file = Files.createFile(newSegmentPath);
            return new Segment(segmentId, file);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Segment createTemporarySegment() {
        var id = maxSegmentId.incrementAndGet();
        return createEmptySegment(id, TEMPORARY_SEGMENT_EXTENSION);
    }

    @Override
    public void deleteTemporarySegment(Segment segment) {
        try {
            Files.deleteIfExists(segment.getPath());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Segment persist(Segment tempSegment) {
        lock.lock();
        try {
            var id = maxSegmentId.incrementAndGet();
            var newSegmentName = String.format(SEGMENT_TEMPLATE, PREFIX, id, EXTENSION);
            var newSegmentPath = root.resolve(newSegmentName);
            try {
                Files.move(tempSegment.getPath(), newSegmentPath, StandardCopyOption.ATOMIC_MOVE);
                var segment = new Segment(id, newSegmentPath, tempSegment.getKeys());
                segments.add(segment);
                log.info("Added new segment {}", segment);
                log.info(segments.toString());
                return segment;
            } catch (IOException e) {
                log.error("Failed to persist segment", e);
                throw new RuntimeException("Failed to persist segment", e);
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void deleteSegment(Segment segment) {
        lock.lock();
        try {
            Files.deleteIfExists(segment.getPath());
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            segments.remove(segment);
            lock.unlock();
        }
    }

    @Override
    public List<Segment> getAllSegments() {
        return List.copyOf(segments);
    }

    @Override
    public Segment getCurrent() {
        return current;
    }

    @Override
    public void setCurrent(Segment segment) {
        Objects.requireNonNull(segment);
        this.current = segment;
    }

    @Override
    public int getMaxSegmentSize() {
        return segmentSize;
    }

    @Override
    public boolean isCompactable() {
        return segments.size() > MAX_SEGMENT_COUNT;
    }

    @Override
    public List<Segment> getOldSegments() {
        if(segments.size() > MAX_SEGMENT_COUNT) {
            return segments.stream()
                    .sorted(Comparator.comparingInt(Segment::getId))
                    .limit(OLD_SEGMENTS_COUNT)
                    .toList();
        }
        throw new RuntimeException("Segment count does not exceed max segment count size " + MAX_SEGMENT_COUNT);
    }

    @Override
    public void addSegmentListener(SegmentListener listener) {
        listeners.add(listener);
    }

    @Override
    public void notify(Segment segment) {
        for (SegmentListener listener : listeners) {
            log.info("Notifying listener {}", listener.getClass().getSimpleName());
            listener.onSegmentFull(segment);
        }
    }
}
