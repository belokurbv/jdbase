package com.belokur.jldbase.api;

import java.nio.file.Path;
import java.util.BitSet;
import java.util.Objects;

public final class Segment {
    private final int id;
    private final Path path;
    private final BitSet keys;

    public Segment(int id, Path path,) {
        this.id = id;
        this.path = path;
        this.keys = new BitSet();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj == null || obj.getClass() != this.getClass()) return false;
        var that = (Segment) obj;
        return this.id == that.id &&
                Objects.equals(this.path, that.path) &&
                Objects.equals(this.keys, that.keys);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, path, keys);
    }

    @Override
    public String toString() {
        return "Segment[" +
                "id=" + id + ", " +
                "path=" + path + ", " +
                "keys=" + keys + ']';
    }

    public int getId() {
        return id;
    }

    public Path getPath() {
        return path;
    }

    public void addKey(int key) {
        keys.set(key);
    }

    public void clearKey(int key) {
        keys.clear(key);
    }

    public BitSet getKeys() {
        return keys;
    }
}
