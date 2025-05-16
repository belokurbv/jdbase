package com.belokur.jldbase.impl.reader;

import com.belokur.jldbase.api.DataReader;

import java.io.Closeable;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

public class DataReaderV1 implements Closeable, DataReader {
    public static final int CAPACITY = 4;
    FileInputStream fis;
    FileChannel channel;

    public DataReaderV1(Path path) throws FileNotFoundException {
        this.fis = new FileInputStream(path.toFile());
        this.channel = fis.getChannel();
    }

    @Override
    public int readSize() throws IOException {
        var lenBuf = ByteBuffer.allocate(CAPACITY);
        channel.read(lenBuf);
        lenBuf.flip();
        return lenBuf.getInt();
    }

    @Override
    public String readValue(int len) throws IOException {
        var recordBuf = ByteBuffer.allocate(len);
        channel.read(recordBuf);
        recordBuf.flip();
        return new String(recordBuf.array(), StandardCharsets.UTF_8);
    }

    @Override
    public long position() throws IOException {
        return channel.position();
    }

    @Override
    public void setPosition(long pos) throws IOException {
        channel.position(pos);
    }

    @Override
    public long size() throws IOException {
        return channel.size();
    }

    @Override
    public void close() throws IOException {
        channel.close();
        fis.close();
    }
}

