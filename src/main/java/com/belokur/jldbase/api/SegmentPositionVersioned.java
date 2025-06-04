package com.belokur.jldbase.api;

public class SegmentPositionVersioned extends SegmentPosition {
    private long version;

    public SegmentPositionVersioned(Segment segment, int position, long version) {
        super(segment, position);
    }

    public long getVersion() {
        return version;
    }

    @Override
    public String toString() {
        return "SegmentPosition[" +
                "segment=" + getSegment() + ", " +
                "position=" + getPosition() + ", " +
                "version=" + version + "]";
    }
}
