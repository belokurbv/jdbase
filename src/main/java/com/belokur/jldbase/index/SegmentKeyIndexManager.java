package com.belokur.jldbase.index;

import com.belokur.jldbase.api.SegmentPosition;
import com.belokur.jldbase.api.SegmentPositionVersioned;

import java.util.Map;

public interface SegmentKeyIndexManager {

    SegmentPositionVersioned get(String key);

    void put(String key, SegmentPositionVersioned pos);

  //  void resetSnapshot();

   //  void mergeSnapshot(Map<String, SegmentPositionVersioned> newMap);

//     Map<String, SegmentPositionVersioned> getSnapshot();

    Map<String, SegmentPositionVersioned> getMemoryMap();

    Long incrementAndGetVersion();

    Long getVersion();

    void putAll(Map<String, SegmentPositionVersioned> map);
}
