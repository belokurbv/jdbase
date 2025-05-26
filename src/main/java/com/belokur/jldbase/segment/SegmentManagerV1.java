package com.belokur.jldbase.segment;

import com.belokur.jldbase.api.Segment;
import com.belokur.jldbase.api.SegmentManager;
import com.belokur.jldbase.storage.LogStorageV4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class SegmentManagerV1 implements SegmentManager {
    private static final Logger log = LoggerFactory.getLogger(LogStorageV4.class);
    public static final String EXTENSION = ".dat";
    public static final String TEMPORARY_SEGMENT_EXTENSION = ".tmp";
    public static final String PREFIX = "segment";
    public static final String SEGMENT_TEMPLATE = "%s-%08d%s";
    private int maxSegmentId;
    private final List<Segment> segments;
    private Segment current;
    private final Path root;

    public SegmentManagerV1(Path root) {
        this.root = root;
        this.segments = new ArrayList<>();
        this.maxSegmentId = 0;
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
            this.maxSegmentId = segments
                    .stream()
                    .map(Segment::getId)
                    .mapToInt(Integer::intValue)
                    .max()
                    .orElse(0);
        } catch (IOException e) {
            throw new RuntimeException("Can't get segments from folder" + e);
        }
    }

    @Override
    public Segment addSegment() {
        maxSegmentId++;
        var segment = createEmptySegment(maxSegmentId, EXTENSION);
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
        maxSegmentId++;
        return createEmptySegment(maxSegmentId, TEMPORARY_SEGMENT_EXTENSION);
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
        this.maxSegmentId++;
        var newSegmentName = String.format(SEGMENT_TEMPLATE, PREFIX, this.maxSegmentId, EXTENSION);
        var newSegmentPath = root.resolve(newSegmentName);
        try {
            Files.move(tempSegment.getPath(), newSegmentPath, StandardCopyOption.ATOMIC_MOVE);
            var segment = new Segment(this.maxSegmentId, newSegmentPath, tempSegment.getKeys());
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
        return segments;
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
