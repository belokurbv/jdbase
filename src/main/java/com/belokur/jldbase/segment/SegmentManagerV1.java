package com.belokur.jldbase.segment;

import com.belokur.jldbase.api.Segment;
import com.belokur.jldbase.api.SegmentManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

public class SegmentManagerV1 implements SegmentManager {
    public static final String EXTENSION = ".dat";
    public static final String TEMPORARY_SEGMENT_EXTENSION = ".tmp";
    public static final String PREFIX = "segment";
    public static final String SEGMENT_TEMPLATE = "%s-%08d%s";
    private static final Logger log = LoggerFactory.getLogger(SegmentManagerV1.class);
    private final AtomicInteger maxSegmentId;
    private final List<Segment> segments;
    private final Path root;
    private volatile Segment current;

    public SegmentManagerV1(Path root) {
        this.root = root;
        this.segments = new CopyOnWriteArrayList<>();
        this.maxSegmentId = new AtomicInteger();
        initSegments(root);
        this.current = addSegment();
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
                    .filter(p -> p.toString().endsWith(EXTENSION))
                    .map(file -> new Segment(getSegmentId(file), file))
                    .toList();
            this.segments.addAll(fileSegments);
            var currMax = segments
                    .stream()
                    .map(Segment::getId)
                    .mapToInt(Integer::intValue)
                    .max()
                    .orElse(0);
            maxSegmentId.set(currMax);
        } catch (IOException e) {
            throw new RuntimeException("Can't get segments from folder" + e);
        }
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
    public void persist(Segment tempSegment) {
        var id = maxSegmentId.incrementAndGet();
        var newSegmentName = String.format(SEGMENT_TEMPLATE, PREFIX, id, EXTENSION);
        var newSegmentPath = root.resolve(newSegmentName);
        try {
            Files.move(tempSegment.getPath(), newSegmentPath, StandardCopyOption.ATOMIC_MOVE);
            var segment = new Segment(id, newSegmentPath, tempSegment.getKeys());
            segments.add(segment);
        } catch (IOException e) {
            log.error("Failed to persist segment", e);
            throw new RuntimeException("Failed to persist segment", e);
        }
    }

    @Override
    public void deleteSegment(Segment segment) {
        try {
            Files.deleteIfExists(segment.getPath());
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            segments.remove(segment);
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
}
