package cn.oddworld.store;

import cn.oddworld.common.AppendMessageStatus;
import cn.oddworld.common.CommonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

public class CommitMapFile {

    private Logger log = LoggerFactory.getLogger(CommitMapFile.class);
    private final MapFileQueue mapFileQueue;
    private final ConsumeMapFile consumeMapFile;
    private final String business;
    private final String storePath;
    private final int mappedFileSize;
    private ReentrantLock lock;

    public CommitMapFile(String business, String storePath, int mappedFileSize, ConsumeMapFile consumeMapFile) {
        this.mapFileQueue = new MapFileQueue(storePath, mappedFileSize);
        this.business = business;
        this.storePath = storePath;
        this.mappedFileSize = mappedFileSize;
        this.consumeMapFile = consumeMapFile;
        lock = new ReentrantLock();
    }

    public MapFileElemResult getData(final long offset) {
        return this.getData(offset, offset == 0);
    }

    public MapFileElemResult getData(final long offset, final boolean returnFirstOnNotFound) {
        MapFile mappedFile = this.mapFileQueue.findMappedFileByOffset(offset, returnFirstOnNotFound);
        if (mappedFile != null) {
            int pos = (int) (offset % mappedFileSize);
            MapFileElemResult result = mappedFile.selectMappedBuffer(pos);
            return result;
        }

        return null;
    }

    public MapFileElemResult getMessage(final long offset, final int size) {
        MapFile mappedFile = this.mapFileQueue.findMappedFileByOffset(offset, offset == 0);
        if (mappedFile != null) {
            int pos = (int) (offset % mappedFileSize);
            return mappedFile.selectMappedBuffer(pos, size);
        }
        return null;
    }

    public Message getMessageContent(final long offset, final int size){

        final MapFileElemResult message = getMessage(offset, size);
        return CommonUtils.buffer2Message(message.getByteBuffer());
    }


    public boolean appendMessage(Message message){
        MapFile lastMapFile = mapFileQueue.getLastMapFile(0);
        lock.lock();
        try {
            final AppendMessageResult messageResult = appendMessage(lastMapFile, message);
            switch (messageResult.getStatus()){
                case PUT_OK:
                    consumeMapFile.appendMessage(messageResult.getWrotePos()+lastMapFile.getFileFromOffset(), messageResult.getWroteBytes());
                    break;
                case END_OF_FILE:
                    lastMapFile = mapFileQueue.getLastMapFile(0);
                    final AppendMessageResult result = appendMessage(lastMapFile, message);
                    if(!result.getStatus().equals(AppendMessageStatus.PUT_OK)){
                        log.error("appendMessage error, msg = {}", message);
                        return false;
                    }else {
                        consumeMapFile.appendMessage(result.getWrotePos()+lastMapFile.getFileFromOffset(), result.getWroteBytes());
                    }
                    break;
                default:
                    log.warn("appendMessage un know status error, msg = {}", message);
                    return false;
            }
        }catch (Exception e){
            log.error("appendMessage error, msg = {}", message, e);
        }finally {
            lock.unlock();
        }
        return  true;
    }

    public AppendMessageResult appendMessage(MapFile mapFile, Message message){

        int currentPosition = mapFile.wrotePosition.get();
        int blankSize = mapFile.fileSize - currentPosition;
        if(blankSize > 0){
            final String business = message.getBusiness();
            final byte[] businessBytes = business.getBytes();
            int businessBytesLent = businessBytes.length;

            final byte[] body = message.getBody();
            int bodyLent = body.length;

            final Map<String, String> properties = message.getProperties();
            final String properties2String = CommonUtils.properties2String(properties);
            final byte[] properties2StringBytes = properties2String.getBytes();
            int prosBytesLent = properties2StringBytes.length;

            int msgLent = 4 + businessBytesLent + 4 + bodyLent + 4 + prosBytesLent;
            if(blankSize < msgLent){
                // it is not enough to set the msg, just ignore remaining space
                mapFile.wrotePosition.addAndGet(blankSize);
                log.warn("file space not enough, fill the space, then get next file to handle msg, msg = {}", message);
                return new AppendMessageResult(AppendMessageStatus.END_OF_FILE,  blankSize, currentPosition, System.currentTimeMillis());
            }

            ByteBuffer buffer = ByteBuffer.allocate(msgLent);
            // 1、setting business name info
            buffer.putInt(businessBytesLent);
            buffer.put(businessBytes);

            // 2、setting actual content
            buffer.putInt(bodyLent);
            buffer.put(body);

            // 3、setting extra pros
            buffer.putInt(prosBytesLent);
            buffer.put(properties2StringBytes);
            try {
                buffer.flip();
                mapFile.fileChannel.position(currentPosition);
                mapFile.fileChannel.write(buffer);
            }catch (Throwable throwable){
                log.error("Error occurred when append message to mappedFile.", throwable);
            }
            mapFile.wrotePosition.addAndGet(msgLent);
            return new AppendMessageResult(AppendMessageStatus.PUT_OK, msgLent, currentPosition, System.currentTimeMillis());
        }
        return new AppendMessageResult(AppendMessageStatus.UNKNOWN_ERROR);
    }


    public MapFileQueue getMapFileQueue() {
        return mapFileQueue;
    }

    public String getBusiness() {
        return business;
    }

    public String getStorePath() {
        return storePath;
    }

    public int getMappedFileSize() {
        return mappedFileSize;
    }
}
