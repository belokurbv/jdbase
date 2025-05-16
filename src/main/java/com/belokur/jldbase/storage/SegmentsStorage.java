package com.belokur.jldbase.storage;

import com.belokur.jldbase.api.KeyValueCodec;
import com.belokur.jldbase.api.KeyValueStorage;
import com.belokur.jldbase.api.Segment;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

public abstract class SegmentsStorage implements KeyValueStorage {
    public static final String EXTENSION = ".dat";
    public static final String PREFIX = "segment-";
    public static final String SEGMENT_TEMPLATE = "%s-%08d%s";
    protected List<Segment> segments;
    protected KeyValueCodec codec;
    protected Path root;
    int maxSegmentId;
    int maxSegmentSize = 100;
    protected Segment current;

    public SegmentsStorage(String path, KeyValueCodec codec) {
        this.codec = codec;
        var root = Path.of(path);

        if (!Files.isDirectory(root)) {
            throw new IllegalArgumentException("Path " + path + " should be a valid directory");
        }

        try (var files = Files.list(root)) {
            this.segments = files
                    .filter(p -> p.toString().endsWith(EXTENSION))
                    .map(file -> new Segment(getSegmentId(file), file))
                    .toList();

            this.maxSegmentId = this.segments
                    .stream()
                    .map(Segment::getId)
                    .mapToInt(Integer::intValue)
                    .max()
                    .orElse(0);
            this.current = createSegment();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        init();
    }

    public static int getSegmentId(Path segment) {
        var segmentId = segment.getFileName()
                .toString()
                .replace(PREFIX, "")
                .replace(EXTENSION, "");

        return Integer.parseInt(segmentId);
    }

    public Segment createSegment() {
        var nextSegmentId = maxSegmentId + 1;
        var newSegmentName = String.format(SEGMENT_TEMPLATE, PREFIX, nextSegmentId, EXTENSION);
        var newSegmentPath = root.resolve(newSegmentName);
        try {
            var file = Files.createFile(newSegmentPath);
            var segment = new Segment(nextSegmentId, file);
            segments.add(segment);
            return segment;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void deleteSegment(Segment segment) {
        try {
            Files.deleteIfExists(segment.getPath());
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            segments.remove(segment);
        }
    }
}
