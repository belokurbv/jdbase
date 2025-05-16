package com.belokur.jldbase.impl.reader;

import com.belokur.jldbase.api.DataWriter;

import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;

public class DataWriterV1 implements DataWriter, Closeable {
    private final FileOutputStream fis;
    private final FileChannel channel;

    public DataWriterV1(Path path) throws FileNotFoundException {
        this.fis = new FileOutputStream(path.toFile(), true);
        this.channel = fis.getChannel();
    }

    @Override
    public void append(byte[] content) throws IOException {
        var buffer = ByteBuffer.wrap(content);
        channel.write(buffer);
    }

    @Override
    public long size() throws IOException {
        return channel.size();
    }

    @Override
    public void setPosition(long pos) throws IOException {
        channel.position(pos);
    }

    @Override
    public void close() throws IOException {
        channel.close();
        fis.close();
    }
}
