package com.belokur.jldbase.api;

import java.io.IOException;

public interface DataWriter {
    public void append(byte[] content) throws IOException;

    long size() throws IOException;

    void setPosition(long pos) throws IOException;
}
