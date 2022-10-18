package cn.oddworld.store;

import cn.oddworld.common.AppendMessageStatus;
import cn.oddworld.common.CommonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

public class MapFile {

    private static Logger log = LoggerFactory.getLogger(MapFile.class);
    protected final AtomicInteger wrotePosition = new AtomicInteger(0);
    protected int fileSize;
    protected FileChannel fileChannel;
    private String fileName;
    private File file;
    private long fileFromOffset;
    private MappedByteBuffer mappedByteBuffer;

    public MapFile(final String fileName, final int fileSize) throws IOException {
        init(fileName, fileSize);
    }

    private void init(final String fileName, final int fileSize) throws IOException {
        this.fileName = fileName;
        this.fileSize = fileSize;
        this.file = new File(fileName);
        this.fileFromOffset = Long.parseLong(this.file.getName());
        boolean ok = false;

        ensureDirOK(this.file.getParent());

        try {
            this.fileChannel = new RandomAccessFile(this.file, "rw").getChannel();
            this.mappedByteBuffer = this.fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, fileSize);
            ok = true;
        } catch (FileNotFoundException e) {
            log.error("Failed to create file {} ", this.fileName, e);
            throw e;
        } catch (IOException e) {
            log.error("Failed to map file {} ", this.fileName, e);
            throw e;
        } finally {
            if (!ok && this.fileChannel != null) {
                this.fileChannel.close();
            }
        }
    }


    public int getWrotePosition() {
        return wrotePosition.get();
    }

    public MapFileElemResult selectMappedBuffer(int pos, int size) {
        int readPosition = getReadPosition();
        if ((pos + size) <= readPosition) {
            ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
            byteBuffer.position(pos);
            ByteBuffer byteBufferNew = byteBuffer.slice();
            byteBufferNew.limit(size);
            return new MapFileElemResult(this.fileFromOffset + pos, byteBufferNew, size, this);
        } else {
            log.warn("selectMappedBuffer request pos invalid, request pos: " + pos + ", size: " + size
                    + ", fileFromOffset: " + this.fileFromOffset);
        }
        return null;
    }

    public MapFileElemResult selectMappedBuffer(int pos) {
        int readPosition = getReadPosition();
        if (pos < readPosition && pos >= 0) {
            ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
            byteBuffer.position(pos);
            int size = readPosition - pos;
            ByteBuffer byteBufferNew = byteBuffer.slice();
            byteBufferNew.limit(size);
            return new MapFileElemResult(this.fileFromOffset + pos, byteBufferNew, size, this);
        }
        return null;
    }

    public int getReadPosition() {
        return this.wrotePosition.get();
    }

    public AppendMessageResult appendMessage(MapFile mapFile, Message message){

        int currentPosition = mapFile.wrotePosition.get();
        int blankSize = mapFile.fileSize - currentPosition;
        if(blankSize > 0){
            ByteBuffer buffer = mapFile.getBufferSlice();
            buffer.position(currentPosition);

            final String business = message.getBusiness();
            final byte[] businessBytes = business.getBytes();
            int businessBytesLent = businessBytes.length;

            final byte[] body = message.getBody();
            int bodyLent = body.length;

            final Map<String, String> properties = message.getProperties();
            final String properties2String = CommonUtils.messageProperties2String(properties);
            final byte[] properties2StringBytes = properties2String.getBytes();
            int prosBytesLent = properties2StringBytes.length;

            int msgLent = businessBytesLent + bodyLent + prosBytesLent;
            if(blankSize < msgLent){
                // it is not enough to set the msg, just ignore remaining space
                mapFile.wrotePosition.addAndGet(blankSize);
                log.warn("appendMessage error, msg = {}", message);
                return new AppendMessageResult(AppendMessageStatus.END_OF_FILE,  blankSize, System.currentTimeMillis());
            }

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
                this.fileChannel.position(currentPosition);
                this.fileChannel.write(buffer);
            }catch (Throwable throwable){
                log.error("Error occurred when append message to mappedFile.", throwable);
            }
            mapFile.wrotePosition.addAndGet(msgLent);
            return new AppendMessageResult(AppendMessageStatus.PUT_OK, msgLent, System.currentTimeMillis());
        }
        return new AppendMessageResult(AppendMessageStatus.UNKNOWN_ERROR);
    }



    public boolean appendMessage(final byte[] data) {
        int currentPos = this.wrotePosition.get();

        if ((currentPos + data.length) <= this.fileSize) {
            try {
                this.fileChannel.position(currentPos);
                this.fileChannel.write(ByteBuffer.wrap(data));
            } catch (Throwable e) {
                log.error("Error occurred when append message to mappedFile.", e);
            }
            this.wrotePosition.addAndGet(data.length);
            return true;
        }
        return false;
    }

    public boolean appendMessage(final byte[] data, final int offset, final int length) {
        int currentPos = this.wrotePosition.get();

        if ((currentPos + length) <= this.fileSize) {
            try {
                this.fileChannel.position(currentPos);
                this.fileChannel.write(ByteBuffer.wrap(data, offset, length));
            } catch (Throwable e) {
                log.error("Error occurred when append message to mappedFile.", e);
            }
            this.wrotePosition.addAndGet(length);
            return true;
        }
        return false;
    }

    public static void ensureDirOK(final String dirName) {
        if (dirName != null) {
            File f = new File(dirName);
            if (!f.exists()) {
                boolean result = f.mkdirs();
                log.info(dirName + " mkdir " + (result ? "OK" : "Failed"));
            }
        }
    }

    public long getLastModifiedTimestamp(){

        return this.file.lastModified();
    }

    public boolean isFull() {
        return this.fileSize == this.wrotePosition.get();
    }

    public long getFileFromOffset() {
        return this.fileFromOffset;
    }

    public MappedByteBuffer getMappedByteBuffer() {
        return mappedByteBuffer;
    }

    public ByteBuffer  getBufferSlice(){

        return this.mappedByteBuffer.slice();
    }
}
