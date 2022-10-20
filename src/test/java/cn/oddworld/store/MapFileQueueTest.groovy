package cn.oddworld.store

import org.junit.Before
import org.junit.Test

import java.nio.ByteBuffer

class MapFileQueueTest  {

    private CommitMapFile commitMapFile;
    private ConsumeMapFile consumeMapFile;
    @Before
    void init(){
        consumeMapFile = new ConsumeMapFile("idxfile", "E:\\index\\idxfile", 1024);
        commitMapFile = new CommitMapFile("idxfile", "E:\\index", 1024, consumeMapFile );
    }

    @Test
    void testConsumeAppendMessage() {
        for(int i = 0; i < 2; i++){
            consumeMapFile.appendMessage(i,2*(i+1))
        }
        MapFileElemResult rsp = consumeMapFile.getIndexBuffer(1);
        ByteBuffer buffer = rsp.getByteBuffer();
        long commitSize = buffer.getLong();
        int size = buffer.getInt();

        Message messagex = commitMapFile.getMessageContent(commitSize, size);
        assert  messagex.getBusiness().endsWith(message.getBusiness())
    }

    @Test
    void testAppendMessage() {
        for(int i = 0; i < 15; i++){
            Message message = new Message();
            message.setBody("jeffchan".getBytes())
            message.setBusiness("idx-file");
            Map<String, String> pros = new HashMap<String, String>();
            message.setProperties(pros)
            pros.put("jeffchan1"+i, "jeffValue1"+i);
            pros.put("jeffchan2"+i, "jeffValue2"+i)
            boolean  flag = commitMapFile.appendMessage(message);
            assert flag == true
        }
        MapFileElemResult rsp = consumeMapFile.getIndexBuffer(13);
        ByteBuffer buffer = rsp.getByteBuffer();
        long commitOffset = buffer.getLong();
        int size = buffer.getInt();

        Message messagex = commitMapFile.getMessageContent(commitOffset, size);
        println messagex.getProperties()
    }
}
