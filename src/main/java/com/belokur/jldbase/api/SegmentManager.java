package com.belokur.jldbase.api;

import java.nio.file.Path;
import java.util.List;

public interface SegmentManager {
    int DEFAULT_SEGMENT_SIZE = 1024;

    void initSegments(Path root);

    Segment addSegment();

    Segment createTemporarySegment();

    void deleteTemporarySegment(Segment segment);

    Segment persist(Segment tempSegment);

    void deleteSegment(Segment segment);

    List<Segment> getAllSegments();

    Segment getCurrent();

    void setCurrent(Segment segment);

    int getMaxSegmentSize();

    boolean isCompactable();

    List<Segment> getOldSegments();

    default void addSegmentListener(SegmentListener listener) {};

    default void notify(Segment segment) {};
}
