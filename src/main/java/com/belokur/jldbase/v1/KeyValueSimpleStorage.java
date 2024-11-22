package com.belokur.jldbase.v1;

import com.belokur.jldbase.api.KeyValueStorage;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class KeyValueSimpleStorage implements KeyValueStorage {
    private Path path;

    private Map<String, Long> memoryMap = new ConcurrentHashMap<>();


    public KeyValueSimpleStorage(String path) {
        this.path = Path.of(path);
    }

    static String toRow(String key, String value) {
        return key + "=" + value;
    }

    @Override
    public void set(String key, String value) {
        var content = toRow(key, value);

        try (var file = new FileOutputStream(path.toFile());
             var channel = file.getChannel()) {
            memoryMap.put(key, channel.size());
            channel.position(channel.size());
            var buffer = ByteBuffer.wrap(content.getBytes());
            channel.write(buffer);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String get(String key) {
        if (!memoryMap.containsKey(key)) {
            return null;
        }

        try (var file = new RandomAccessFile(path.toFile(), "r");
             var channel = file.getChannel()) {
            channel.position(memoryMap.get(key));

            ByteBuffer buffer = ByteBuffer.allocate(1024);
            StringBuilder line = new StringBuilder();
            int bytesRead;

            while ((bytesRead = channel.read(buffer)) > 0) {
                buffer.flip(); // Switch buffer to read mode

                while (buffer.hasRemaining()) {
                    char ch = (char) buffer.get();
                    if (ch == '\n') {
                        line.setLength(0);
                    } else {
                        line.append(ch);
                    }
                }
                buffer.clear();
            }

            return line.toString();

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
