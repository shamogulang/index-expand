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
    void testAppendMessage() {
        Message message = new Message();
        message.setBody("jeffchan".getBytes())
        message.setBusiness("idx-file");
        Map<String, String> pros = new HashMap<String, String>();
        pros.put("jeffchan1", "jeffValue1");
        pros.put("jeffchan2", "jeffValue2")
        message.setProperties(pros)
        for(int i = 0; i < 1; i++){
            boolean  flag = commitMapFile.appendMessage(message);
            assert flag == true
        }
        MapFileElemResult rsp = consumeMapFile.getIndexBuffer(0);
        ByteBuffer buffer = rsp.getByteBuffer();
        long commitSize = buffer.getLong();
        int size = buffer.getInt();

        Message messagex = commitMapFile.getMessageContent(commitSize, size);
        assert  messagex.getBusiness().endsWith(message.getBusiness())
    }
}
