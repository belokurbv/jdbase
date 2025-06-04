package com.belokur.jldbase.api;

import java.util.Objects;

public class SegmentPosition {
    private final Segment segment;
    private final int position;

    public SegmentPosition(Segment segment, int position) {
        this.segment = segment;
        this.position = position;
    }

    public Segment getSegment() {
        return segment;
    }

    public int getPosition() {
        return position;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj == null || obj.getClass() != this.getClass()) return false;
        var that = (SegmentPosition) obj;
        return Objects.equals(this.segment, that.segment) &&
                this.position == that.position;
    }

    @Override
    public int hashCode() {
        return Objects.hash(segment, position);
    }

    @Override
    public String toString() {
        return "SegmentPosition[" +
                "segment=" + segment + ", " +
                "position=" + position + ']';
    }

}
