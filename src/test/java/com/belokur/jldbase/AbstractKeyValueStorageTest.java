package com.belokur.jldbase;

import com.belokur.jldbase.api.KeyValueStorage;
import com.belokur.jldbase.exception.KeyException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

abstract class AbstractKeyValueStorageTest {
    @TempDir
    Path tempDir;
    KeyValueStorage storage;

    protected abstract KeyValueStorage createStorage(Path path);

    @BeforeEach
    void setUp() {
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

    @Test
    void shouldThrowException_WhenKeyDoesNotExistInStorage() {
        assertThrows(KeyException.class, () -> storage.get("foo"));
    }

    @Test
    void shouldLoadValues_WhenDatabaseFileExists() {
        storage.set("foo", "bar");
        storage.set("key", "oldValue");
        var secondaryStorage = createStorage(tempDir);
        assertEquals("bar", secondaryStorage.get("foo"));
        assertEquals("oldValue", secondaryStorage.get("key"));
    }
}

