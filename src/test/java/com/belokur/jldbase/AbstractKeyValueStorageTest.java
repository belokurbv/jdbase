package com.belokur.jldbase;

import com.belokur.jldbase.api.KeyValueStorage;
import com.belokur.jldbase.exception.KeyException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

abstract class AbstractKeyValueStorageTest {
    @TempDir
    Path tempDir;
    KeyValueStorage storage;

    protected abstract KeyValueStorage createStorage(Path filePath);

    @BeforeEach
    void setUp() {
        System.out.println("Temp dir: " + tempDir);
        assertTrue(Files.isDirectory(tempDir));
        storage = createStorage(tempDir);
    }

    @Test
    void shouldReturnStoredValue_WhenKeyExists() {
        storage.set("foo", "bar");
        assertEquals("bar", storage.get("foo"));
    }

    @Test
    void shouldReturnLatestValue_WhenKeyIsOverwritten() {
        storage.set("key", "oldValue");
        storage.set("key", "newValue");
        assertEquals("newValue", storage.get("key"));
    }

    @Test
    void shouldThrowException_WhenKeyDoesNotExist() {
        assertThrows(KeyException.class, () -> storage.get("missingKey"));
    }
}

