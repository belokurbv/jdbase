package com.belokur.jldbase.v1;

import com.belokur.jldbase.api.KeyValueStorage;
import com.belokur.jldbase.exception.KeyException;
import com.belokur.jldbase.impl.extractors.BinaryValueExtractor;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class LogStorageV2 extends SingleFileStorage implements KeyValueStorage {
    public static final String DEFAULT_FILE_NAME = "db_v2.dat";

    private final Map<String, Long> memoryMap = new ConcurrentHashMap<>();

    public LogStorageV2(String path) {
        super(path, DEFAULT_FILE_NAME, new BinaryValueExtractor());
    }

    @Override
    public void set(String key, String value) {
        var content = extractor.toRecord(key, value);

        try (var file = new FileOutputStream(path.toFile(), true);
             var channel = file.getChannel()) {
            memoryMap.put(key, channel.size());
            channel.position(channel.size());
            var buffer = ByteBuffer.wrap(content);
            channel.write(buffer);
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

        try (RandomAccessFile raf = new RandomAccessFile(path.toFile(), "r")) {
            raf.seek(position);
            int length = raf.readInt();
            byte[] data = new byte[length];
            raf.readFully(data);
            var content = new String(data);
            return extractor.fromRecord(content).value();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
