package com.belokur.jldbase.api;

import java.nio.file.Path;
import java.util.BitSet;
import java.util.Objects;

public final class Segment {
    private final int id;
    private final Path path;
    private final BitSet keys;
    private boolean dirty;

    public Segment(int id, Path path) {
        this.id = id;
        this.path = path;
        this.keys = new BitSet();
        this.dirty = false;
    }

    public Segment(int id, Path path, BitSet keys) {
        this.id = id;
        this.path = path;
        this.keys = keys;
        this.dirty = false;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        Segment that = (Segment) obj;
        return id == that.id;
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

    public void dirty() {
        this.dirty = true;
    }

    public boolean isDirty() {
        return dirty;
    }
}
