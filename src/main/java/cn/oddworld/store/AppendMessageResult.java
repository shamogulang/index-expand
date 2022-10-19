package cn.oddworld.store;

import cn.oddworld.common.AppendMessageStatus;

public class AppendMessageResult {

    private AppendMessageStatus status;
    // Write Bytes
    private int wroteBytes;
    private int wrotePos;
    // Message storage timestamp
    private long storeTimestamp;

    public AppendMessageResult(AppendMessageStatus status) {
        this(status,  0, 0,System.currentTimeMillis());
    }

    public AppendMessageResult(AppendMessageStatus status, int wroteBytes, int wrotePos,
        long storeTimestamp) {
        this.status = status;
        this.wroteBytes = wroteBytes;
        this.wrotePos = wrotePos;
        this.storeTimestamp = storeTimestamp;
    }

    public AppendMessageStatus getStatus() {
        return status;
    }

    public void setStatus(AppendMessageStatus status) {
        this.status = status;
    }

    public int getWroteBytes() {
        return wroteBytes;
    }

    public void setWroteBytes(int wroteBytes) {
        this.wroteBytes = wroteBytes;
    }

    public long getStoreTimestamp() {
        return storeTimestamp;
    }

    public void setStoreTimestamp(long storeTimestamp) {
        this.storeTimestamp = storeTimestamp;
    }

    public int getWrotePos() {
        return wrotePos;
    }

    public void setWrotePos(int wrotePos) {
        this.wrotePos = wrotePos;
    }
}
