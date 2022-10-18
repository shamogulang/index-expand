package cn.oddworld.store;

import java.nio.ByteBuffer;

public class MapFileElemResult {

    private final long startOffset;

    private final ByteBuffer byteBuffer;

    private MapFile mapFile;

    private int size;


    public MapFileElemResult(long startOffset, ByteBuffer byteBuffer, int size, MapFile mapFile) {
        this.startOffset = startOffset;
        this.byteBuffer = byteBuffer;
        this.size = size;
        this.mapFile = mapFile;
    }

    public ByteBuffer getByteBuffer() {
        return byteBuffer;
    }

    public int getSize() {
        return size;
    }

    public void setSize(final int s) {
        this.size = s;
        this.byteBuffer.limit(this.size);
    }

    public long getStartOffset() {
        return startOffset;
    }

    public MapFile getMapFile() {
        return mapFile;
    }
}
