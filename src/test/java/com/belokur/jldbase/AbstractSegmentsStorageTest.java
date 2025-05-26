package com.belokur.jldbase;

import com.belokur.jldbase.api.KeyValueStorage;
import com.belokur.jldbase.api.Segment;
import com.belokur.jldbase.api.SegmentManager;
import com.belokur.jldbase.segment.SegmentManagerImpl;
import com.belokur.jldbase.storage.LogStorageV4;
import com.belokur.jldbase.storage.SegmentsStorage;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

public abstract class AbstractSegmentsStorageTest extends AbstractKeyValueStorageTest {
    protected SegmentManager segmentManager;

    static void initFixtures(KeyValueStorage storage) {
        storage.set("foo", "1234567890");
        storage.set("key", "1234567890");
        storage.set("abc", "1234567890");
        storage.set("cde", "1234567890");
        storage.set("fgh", "1234567890");
    }

    @Test
    void shouldCreateNewSegment_WhenSegmentSizeIsReached() {
        initFixtures(storage);
        assertEquals(3, segmentManager.getAllSegments().size());
    }

    @Test
    void shouldHaveCleanSegments_WhenAllValuesAreUnique() {
        initFixtures(storage);
        var segments = segmentManager.getAllSegments();
        assertFalse(segments.stream().anyMatch(Segment::isDirty));
    }

    @Test
    void shouldPositionStored_WhenKeysAreAdded() {
        storage.set("foo", "1234567890"); // + 21 bytes
        storage.set("key", "1234567890"); // + 21 bytes
        var current = segmentManager.getCurrent();
        var firstPositionInSegment = current.getKeys().nextSetBit(0);
        var secondPositionInSegment = current.getKeys().nextSetBit(firstPositionInSegment + 1);
        assertEquals(0, firstPositionInSegment);
        assertEquals(21, secondPositionInSegment);
    }


}
