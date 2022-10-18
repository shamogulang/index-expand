package cn.oddworld.store

import org.junit.Before
import org.junit.Test

class MapFileQueueTest  {

    private MapFileQueue mapFileQueue;
    @Before
    void init(){
        mapFileQueue = new MapFileQueue("E:\\index", 1024 * 1024 * 50);
    }

    @Test
    void testGetLastMapFile() {
        MapFile mapFile = mapFileQueue.getLastMapFile(0);
        assert  mapFile != null;
    }

    void testAppendMessage() {
    }
}
