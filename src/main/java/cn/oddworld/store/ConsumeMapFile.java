package cn.oddworld.store;

import cn.oddworld.common.AppendMessageStatus;
import cn.oddworld.common.CommonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

public class ConsumeMapFile {

    private Logger log = LoggerFactory.getLogger(CommitMapFile.class);
    private final static int UNIT_SIZE = 12;
    private final MapFileQueue mapFileQueue;
    private final String business;
    private final String storePath;
    private final int mappedFileSize;
    private ReentrantLock lock;


    public ConsumeMapFile(String business, String storePath, int mappedFileSize) {
        this.mapFileQueue = new MapFileQueue(storePath, mappedFileSize);
        this.business = business;
        this.storePath = storePath;
        this.mappedFileSize = mappedFileSize;
        lock = new ReentrantLock();
    }

    public MapFileElemResult getIndexBuffer(final long startIndex) {
        int mappedFileSize = this.mappedFileSize;
        long offset = startIndex * UNIT_SIZE;
        // 去掉了最小逻辑位置
        MapFile mappedFile = this.mapFileQueue.findMappedFileByOffset(offset);
        if (mappedFile != null) {
            MapFileElemResult result = mappedFile.selectMappedBuffer((int) (offset % mappedFileSize), UNIT_SIZE);
            return result;
        }
        return null;
    }

    public boolean appendMessage(long commitOffset, int msgSize){
        MapFile lastMapFile = mapFileQueue.getLastMapFile(0);
        lock.lock();
        try {
            final AppendMessageResult messageResult = appendMessage(lastMapFile, commitOffset, msgSize);
            switch (messageResult.getStatus()){
                case PUT_OK:
                    break;
                case END_OF_FILE:
                    lastMapFile = mapFileQueue.getLastMapFile(0);
                    final AppendMessageResult result = appendMessage(lastMapFile, commitOffset, msgSize);
                    if(!result.getStatus().equals(AppendMessageStatus.PUT_OK)){
                        log.error("consume file appendMessage error not ok , commitOffset = {}, msgSize = {}", commitOffset, msgSize);
                        return false;
                    }
                    break;
                default:
                    log.warn("consume file appendMessage un know status error,commitOffset = {}, msgSize = {}", commitOffset, msgSize);
                    return false;
            }
        }catch (Exception e){
            log.error("consume file appendMessage error exception, commitOffset = {}, msgSize = {}", commitOffset, msgSize, e);
        }finally {
            lock.unlock();
        }
        return  true;
    }

    public AppendMessageResult appendMessage(MapFile mapFile, long commitOffset, int msgSize){

        int currentPosition = mapFile.wrotePosition.get();
        int blankSize = mapFile.fileSize - currentPosition;
        if(blankSize > 0){

            if(blankSize < UNIT_SIZE){
                // it is not enough to set the msg, just ignore remaining space
                mapFile.wrotePosition.addAndGet(blankSize);
                log.warn("consume file space not enough, fill the space, then get next file to handle msg, commitOffset = {}, msgSize = {}", commitOffset, msgSize);
                return new AppendMessageResult(AppendMessageStatus.END_OF_FILE,  blankSize, currentPosition, System.currentTimeMillis());
            }
            ByteBuffer buffer = ByteBuffer.allocate(UNIT_SIZE);
            // 1、setting commitOffset and message size
            buffer.putLong(commitOffset);
            buffer.putInt(msgSize);
            try {
                buffer.flip();
                while (buffer.hasRemaining()){
                    mapFile.fileChannel.position(currentPosition);
                    mapFile.fileChannel.write(buffer);
                }
            }catch (Throwable throwable){
                log.error("Error occurred when append message to mappedFile.", throwable);
            }
            mapFile.wrotePosition.addAndGet(UNIT_SIZE);
            return new AppendMessageResult(AppendMessageStatus.PUT_OK, UNIT_SIZE, currentPosition, System.currentTimeMillis());
        }
        return new AppendMessageResult(AppendMessageStatus.UNKNOWN_ERROR);
    }


    public long getLastOffset() {
        long lastOffset = -1;
        int logicFileSize = this.mappedFileSize;

        MapFile mapFile = this.mapFileQueue.getLastMapFile();
        if (mapFile != null) {

            int position = mapFile.getWrotePosition() - UNIT_SIZE;
            if (position < 0)
                position = 0;

            ByteBuffer byteBuffer = mapFile.getBufferSlice();
            byteBuffer.position(position);
            for (int i = 0; i < logicFileSize; i += UNIT_SIZE) {
                long offset = byteBuffer.getLong();
                int size = byteBuffer.getInt();

                if (offset >= 0 && size > 0) {
                    lastOffset = offset + size;
                } else {
                    break;
                }
            }
        }
        return lastOffset;
    }
}
