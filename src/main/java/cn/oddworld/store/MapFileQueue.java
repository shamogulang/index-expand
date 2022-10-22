package cn.oddworld.store;

import cn.oddworld.common.AppendMessageStatus;
import cn.oddworld.common.CommonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
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


    private final CopyOnWriteArrayList<MapFile> mapFiles = new CopyOnWriteArrayList<>();

    public MapFileQueue(String storePath, int mappedFileSize) {
        this.storePath = storePath;
        this.mappedFileSize = mappedFileSize;
    }

    public MapFile getLastMapFile(final long startOffset) {
        return getLastMapFile(startOffset, true);
    }

    public MapFile getFirstMapFile() {
        MapFile mappedFileFirst = null;

        if (!this.mapFiles.isEmpty()) {
            try {
                mappedFileFirst = this.mapFiles.get(0);
            } catch (IndexOutOfBoundsException e) {
                //ignore
            } catch (Exception e) {
                log.error("getFirstMappedFile has exception.", e);
            }
        }

        return mappedFileFirst;
    }

    public boolean load() {
        File dir = new File(this.storePath);
        File[] files = dir.listFiles();
        if (files != null) {
            // ascending order
            Arrays.sort(files);
            for (File file : files) {

                if (file.length() != this.mappedFileSize) {
                    log.warn(file + "\t" + file.length()
                            + " length not matched message store config value, please check it manually");
                    return false;
                }

                try {
                    MapFile mappedFile = new MapFile(file.getPath(), mappedFileSize);
                    mappedFile.wrotePosition.set(this.mappedFileSize);
                    this.mapFiles.add(mappedFile);
                    log.info("load " + file.getPath() + " OK");
                } catch (IOException e) {
                    log.error("load file " + file + " error", e);
                    return false;
                }
            }
        }

        return true;
    }

    public MapFile findMappedFileByOffset(final long offset) {
        return findMappedFileByOffset(offset, false);
    }

    public MapFile findMappedFileByOffset(final long offset, final boolean returnFirstOnNotFound) {
        try {
            MapFile firstMappedFile = this.getFirstMapFile();
            MapFile lastMappedFile = this.getLastMapFile();
            if (firstMappedFile != null && lastMappedFile != null) {
                if (offset < firstMappedFile.getFileFromOffset() || offset >= lastMappedFile.getFileFromOffset() + this.mappedFileSize) {
                    log.warn("Offset not matched. Request offset: {}, firstOffset: {}, lastOffset: {}, mappedFileSize: {}, mappedFiles count: {}",
                            offset,
                            firstMappedFile.getFileFromOffset(),
                            lastMappedFile.getFileFromOffset() + this.mappedFileSize,
                            this.mappedFileSize,
                            this.mapFiles.size());
                } else {
                    int index = (int) ((offset / this.mappedFileSize) - (firstMappedFile.getFileFromOffset() / this.mappedFileSize));
                    MapFile targetFile = null;
                    try {
                        targetFile = this.mapFiles.get(index);
                    } catch (Exception ignored) {
                    }

                    if (targetFile != null && offset >= targetFile.getFileFromOffset()
                            && offset < targetFile.getFileFromOffset() + this.mappedFileSize) {
                        return targetFile;
                    }

                    for (MapFile tmpMappedFile : this.mapFiles) {
                        if (offset >= tmpMappedFile.getFileFromOffset()
                                && offset < tmpMappedFile.getFileFromOffset() + this.mappedFileSize) {
                            return tmpMappedFile;
                        }
                    }
                }

                if (returnFirstOnNotFound) {
                    return firstMappedFile;
                }
            }
        } catch (Exception e) {
            log.error("findMappedFileByOffset Exception", e);
        }

        return null;
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
