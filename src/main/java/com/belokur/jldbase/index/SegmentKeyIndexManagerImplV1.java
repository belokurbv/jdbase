package com.belokur.jldbase.index;

import com.belokur.jldbase.api.SegmentPositionVersioned;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class SegmentKeyIndexManagerImplV1 implements SegmentKeyIndexManager {
    private final ConcurrentHashMap<String, SegmentPositionVersioned> memoryMap;
    private final AtomicLong version;

    public SegmentKeyIndexManagerImplV1() {
        this.memoryMap = new ConcurrentHashMap<>();
        this.version = new AtomicLong(0);
    }


    @Override
    public SegmentPositionVersioned get(String key) {
        return memoryMap.get(key);
    }

    @Override
    public void put(String key, SegmentPositionVersioned pos) {
        memoryMap.put(key, pos);
    }

//    @Override
//    public void resetSnapshot() {
//        snapshotMap = Map.copyOf(memoryMap);
//        memoryMap.clear();
//    }

//    @Override
//    public void mergeSnapshot(Map<String, SegmentPositionVersioned> newMap) {
//        for (var entry : snapshotMap.entrySet()) {
//            newMap.putIfAbsent(entry.getKey(), entry.getValue());
//        }
//    }

//    @Override
//    public Map<String, SegmentPositionVersioned> getSnapshot() {
//        return snapshotMap;
//    }
//
    @Override
    public Map<String, SegmentPositionVersioned> getMemoryMap() {
        return memoryMap;
    }

    @Override
    public Long incrementAndGetVersion() {
        return version.incrementAndGet();
    }

    @Override
    public Long getVersion() {
        return version.get();
    }

    @Override
    public void putAll(Map<String, SegmentPositionVersioned> map) {
        this.memoryMap.putAll(map);
    }
}
