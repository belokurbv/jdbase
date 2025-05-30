package com.belokur.jldbase.index;

import com.belokur.jldbase.api.SegmentPosition;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class SegmentKeyIndexManagerImplV1 implements SegmentKeyIndexManager {
    private final ConcurrentHashMap<String, SegmentPosition> memoryMap = new ConcurrentHashMap<>();
    private volatile Map<String, SegmentPosition> snapshotMap = Map.of();

    @Override
    public SegmentPosition get(String key) {
        SegmentPosition pos = memoryMap.get(key);
        if (pos == null) {
            pos = snapshotMap.get(key);
        }
        return pos;
    }

    @Override
    public void put(String key, SegmentPosition pos) {
        memoryMap.put(key, pos);
    }

    @Override
    public void resetSnapshot() {
        snapshotMap = Map.copyOf(memoryMap);
        memoryMap.clear();
    }

    @Override
    public void mergeSnapshot(Map<String, SegmentPosition> newMap) {
        for (var entry : snapshotMap.entrySet()) {
            newMap.putIfAbsent(entry.getKey(), entry.getValue());
        }
    }

    @Override
    public Map<String, SegmentPosition> getSnapshot() {
        return snapshotMap;
    }

    @Override
    public Map<String, SegmentPosition> getMemoryMap() {
        return memoryMap;
    }
}
