package cn.oddworld.store;

import cn.oddworld.common.AppendMessageStatus;
import cn.oddworld.common.CommonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.File;
import java.io.IOException;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.ReentrantLock;

public class MapFileQueue {

    Logger log = LoggerFactory.getLogger(MapFileQueue.class);

    /**
     * 文件存储的路径
     */
    private final String storePath;

    /**
     * Queue里面每隔文件的大小
     */
    private final int mappedFileSize;

    private ReentrantLock lock;

    private final CopyOnWriteArrayList<MapFile> mapFiles = new CopyOnWriteArrayList<>();

    public MapFileQueue(String storePath, int mappedFileSize) {
        this.storePath = storePath;
        this.mappedFileSize = mappedFileSize;
        lock = new ReentrantLock();
    }

    public MapFile getLastMapFile(final long startOffset) {
        return getLastMapFile(startOffset, true);
    }

    public boolean appendMessage(Message message){
        MapFile lastMapFile = getLastMapFile(0);
        lock.lock();
        try {
            final AppendMessageResult messageResult = lastMapFile.appendMessage(lastMapFile, message);
            switch (messageResult.getStatus()){
                case PUT_OK:
                    break;
                case END_OF_FILE:
                    lastMapFile = getLastMapFile(0);
                    final AppendMessageResult result = lastMapFile.appendMessage(lastMapFile, message);
                    if(!result.getStatus().equals(AppendMessageStatus.PUT_OK)){
                        log.error("appendMessage error, msg = {}", message);
                        return false;
                    }
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

    public MapFile getLastMapFile(final long startOffset, boolean needCreate) {
        long createOffset = -1;
        MapFile mappedFileLast = getLastMapFile();

        if (mappedFileLast == null) {
            createOffset = startOffset - (startOffset % this.mappedFileSize);
        }

        if (mappedFileLast != null && mappedFileLast.isFull()) {
            createOffset = mappedFileLast.getFileFromOffset() + this.mappedFileSize;
        }

        if (createOffset != -1 && needCreate) {
            String nextFilePath = this.storePath + File.separator + CommonUtils.offset2FileName(createOffset);
            MapFile mapFile = null;
            try {
                mapFile = new MapFile(nextFilePath, this.mappedFileSize);
            } catch (IOException e) {
                log.error("create mappedFile exception", e);
            }

            if (mapFile != null) {
                this.mapFiles.add(mapFile);
            }
            return mapFile;
        }

        return mappedFileLast;
    }

    public MapFile getLastMapFile() {
        MapFile mappedFileLast = null;

        while (!this.mapFiles.isEmpty()) {
            try {
                mappedFileLast = this.mapFiles.get(this.mapFiles.size() - 1);
                break;
            } catch (IndexOutOfBoundsException e) {
                //continue;
                log.warn("getLastMappedFile mapFile IndexOutOfBoundsException");
            } catch (Exception e) {
                log.error("getLastMappedFile error, for detail:", e);
                break;
            }
        }

        return mappedFileLast;
    }
}
