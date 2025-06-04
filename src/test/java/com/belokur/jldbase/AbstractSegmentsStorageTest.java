package com.belokur.jldbase;

import com.belokur.jldbase.api.KeyValueStorage;
import com.belokur.jldbase.api.Segment;
import com.belokur.jldbase.api.SegmentManager;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

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

    @Test
    void shouldMergeSegment_WhenKeysAreDuplicated() throws InterruptedException {
        // Initialize duplicate data
        storage.set("foo", "1234567890"); // Duplicate entry 1
        storage.set("foo", "1234567891"); // Duplicate entry 2
        storage.set("foo", "1234567892"); // Duplicate entry 3
        TimeUnit.SECONDS.sleep(2); // This ensures the merge thread finishes execution

        // Check that only one segment remains (i.e., merge happened)
        assertEquals(1, segmentManager.getAllSegments().size(),
                     "Post-merge, only one segment should remain");

        // Validate that the latest value of "foo" is retained
        assertEquals("1234567892", storage.get("foo"),
                     "The latest value should be retained after the merge");

    }
}
