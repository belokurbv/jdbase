package com.belokur.jldbase.storage;

import com.belokur.jldbase.api.KeyValueStorage;
import com.belokur.jldbase.api.Pair;
import com.belokur.jldbase.exception.KeyException;
import com.belokur.jldbase.impl.codec.KeyValueBinaryCodec;
import com.belokur.jldbase.impl.codec.KeyValuePlainBinaryCodec;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class LogStorageV2 extends SingleFileStorage implements KeyValueStorage {
    public static final String DEFAULT_FILE_NAME = "db_v2.dat";

    private Map<String, Long> memoryMap;

    public LogStorageV2(String path) {
        super(path, DEFAULT_FILE_NAME, new KeyValuePlainBinaryCodec());
    }

    public static Pair fromRecord(String row) {
        var array = row.split(",");
        return new Pair(array[0], array[1]);
    }

    public void init() {
        this.memoryMap = new ConcurrentHashMap<>();
        try (var file = new FileInputStream(path.toFile());
             var channel = file.getChannel()) {
            while (channel.position() < channel.size()) {
                var recordStart = channel.position();
                var lenBuf = ByteBuffer.allocate(KeyValueBinaryCodec.CAPACITY);
                channel.read(lenBuf);
                lenBuf.flip();
                var recordLen = lenBuf.getInt();
                var recordBuf = ByteBuffer.allocate(recordLen);
                channel.read(recordBuf);
                recordBuf.flip();
                var line = new String(recordBuf.array(), StandardCharsets.UTF_8);
                var pair = fromRecord(line);
                memoryMap.put(pair.key(), recordStart);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void set(String key, String value) {
        var content = codec.toRecord(key, value);

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

        try (var raf = new RandomAccessFile(path.toFile(), "r")) {
            raf.seek(position);
            var length = raf.readInt();
            var data = new byte[length];
            raf.readFully(data);
            var content = new String(data);
            return fromRecord(content).value();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
