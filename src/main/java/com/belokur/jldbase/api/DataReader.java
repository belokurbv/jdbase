package com.belokur.jldbase.api;

import java.io.IOException;

public interface DataReader {
    int readSize() throws IOException;

    String readValue(int len) throws IOException;

    long position() throws  IOException;

    void setPosition(long pos) throws IOException;

    long size() throws  IOException;
}
