package com.belokur.jldbase.index;

import com.belokur.jldbase.api.SegmentPosition;

import java.util.Map;

public interface SegmentKeyIndexManager {

    SegmentPosition get(String key);

    void put(String key, SegmentPosition pos);

    void resetSnapshot();

    void mergeSnapshot(Map<String, SegmentPosition> newMap);

    Map<String, SegmentPosition> getSnapshot();

    Map<String, SegmentPosition> getMemoryMap();
}
