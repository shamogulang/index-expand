package cn.oddworld.config;

import cn.oddworld.store.CommitMapFile;
import cn.oddworld.store.ConsumeMapFile;
import cn.oddworld.store.MapFile;
import cn.oddworld.store.MapFileQueue;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;

import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;

@Configuration
public class StoreConfiguration {


    @Autowired
    private PathConfiguration pathConfiguration;

    @Autowired
    public CommitMapFile commitMapFile(){

        ConsumeMapFile consumeMapFile = new ConsumeMapFile(pathConfiguration.getConsumeMapFilePath(), pathConfiguration.getConsumeFileSize());
        CommitMapFile commitMapFile = new CommitMapFile(pathConfiguration.getCommitMapFilePath(), pathConfiguration.getCommitFileSize(), consumeMapFile);

        // 加载已经存在的文件
        commitMapFile.load();
        consumeMapFile.load();

        final MapFileQueue mapFileQueue = consumeMapFile.getMapFileQueue();
        final MapFile lastConsumeMapFile = mapFileQueue.getLastMapFile();
        final int mappedFileSize = consumeMapFile.getMappedFileSize();

        final MapFileQueue commitMapFileMapFileQueue = commitMapFile.getMapFileQueue();
        final MapFile lastCommitMapFile = commitMapFileMapFileQueue.getLastMapFile();

        if(lastCommitMapFile == null || lastConsumeMapFile == null){
            return commitMapFile;
        }

        lastConsumeMapFile.setWrotePosition(0);
        lastCommitMapFile.setWrotePosition(0);
        final MappedByteBuffer mappedByteBuffer = lastConsumeMapFile.getMappedByteBuffer();
        final ByteBuffer byteBuffer = mappedByteBuffer.slice();
        for(int i = 0; i < mappedFileSize; i += ConsumeMapFile.UNIT_SIZE){

            final long commitOffset = byteBuffer.getLong();
            final int size = byteBuffer.getInt();
            if(commitOffset >= 0 && size > 0){
                lastConsumeMapFile.setWrotePosition(i+ConsumeMapFile.UNIT_SIZE);
                lastCommitMapFile.setWrotePosition((int)(commitOffset-lastCommitMapFile.getFileFromOffset()) + size);
            }else {
                break;
            }
        }

        return commitMapFile;
    }
}
