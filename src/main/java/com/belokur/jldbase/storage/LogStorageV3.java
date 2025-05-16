package com.belokur.jldbase.storage;

import com.belokur.jldbase.api.KeyValueStorage;
import com.belokur.jldbase.exception.KeyException;
import com.belokur.jldbase.impl.codec.KeyValueBinaryCodec;
import com.belokur.jldbase.impl.reader.DataReaderV1;
import com.belokur.jldbase.impl.writer.DataWriterV1;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class LogStorageV3 extends SingleFileStorage implements KeyValueStorage {
    public static final String DEFAULT_FILE_NAME = "db_v3.dat";

    private Map<String, Long> memoryMap;

    public LogStorageV3(String path) {
        super(path, DEFAULT_FILE_NAME, new KeyValueBinaryCodec());
    }

    public void init() {
        this.memoryMap = new ConcurrentHashMap<>();
        try (var reader = new DataReaderV1(this.path)) {
            while (reader.position() < reader.size()) {
                var recordStart = reader.position();
                var keyLen = reader.readSize();
                var key = reader.readValue(keyLen);
                memoryMap.put(key, recordStart);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void set(String key, String value) {
        var content = codec.toRecord(key, value);

        try (var writer = new DataWriterV1(this.path)) {
            memoryMap.put(key, writer.size());
            writer.setPosition(writer.size());
            writer.append(content);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String get(String key) {
        if (!memoryMap.containsKey(key)) {
            throw new KeyException(key);
        }

        var position = memoryMap.get(key);

        try (var reader = new DataReaderV1(path)) {
            reader.setPosition(position);
            var keySize = reader.readSize();
            var storedKey = reader.readValue(keySize);
            if (!storedKey.equals(key)) {
                throw new KeyException("Key does not match in the storage " + storedKey + " " + key);
            }
            var valSize = reader.readSize();
            return reader.readValue(valSize);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
