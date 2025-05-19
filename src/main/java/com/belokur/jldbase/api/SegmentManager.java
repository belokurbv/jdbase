package com.belokur.jldbase.api;

import java.nio.file.Path;
import java.util.List;

public interface SegmentManager {
    void initSegments(Path root);

    Segment addSegment();

    Segment createTemporarySegment();

    void deleteTemporarySegment(Segment segment);

    void persist(Segment tempSegment);

    void deleteSegment(Segment segment);

    List<Segment> getAllSegments();

    Segment getCurrent();

    void setCurrent(Segment segment);
}
