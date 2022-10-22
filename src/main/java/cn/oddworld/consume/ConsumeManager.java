package cn.oddworld.consume;

import cn.oddworld.common.CommonUtils;
import cn.oddworld.common.JacksonUtil;
import cn.oddworld.config.PathConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@Component
public class ConsumeManager {

    private Logger log = LoggerFactory.getLogger(ConsumeManager.class);
    @Autowired
    private PathConfiguration pathConfiguration;

    private ConcurrentMap<String/* businessName */, Long> offsetTable =
            new ConcurrentHashMap<>(512);

    public boolean load() {
        String fileName = null;
        try {
            fileName = pathConfiguration.getConsumePath();
            String jsonString = CommonUtils.file2String(fileName);

            if (null == jsonString || jsonString.length() == 0) {
                return this.loadBak();
            } else {
                this.decode(jsonString);
                log.info("load " + fileName + " OK");
                return true;
            }
        } catch (Exception e) {
            log.error("load " + fileName + " failed, and try to load backup file", e);
            return this.loadBak();
        }
    }

    private boolean loadBak() {
        String fileName = null;
        try {
            fileName = pathConfiguration.getConsumePath();
            String jsonString = CommonUtils.file2String(fileName + ".bak");
            if (jsonString != null && jsonString.length() > 0) {
                this.decode(jsonString);
                log.info("load " + fileName + " OK");
                return true;
            }
        } catch (Exception e) {
            log.error("load " + fileName + " Failed", e);
            return false;
        }

        return true;
    }

    public synchronized void persist() {
        String jsonString = JacksonUtil.INSTANCE.toJson(true);
        if (jsonString != null) {
            String fileName = pathConfiguration.getConsumePath();
            try {
                CommonUtils.string2File(jsonString, fileName);
            } catch (IOException e) {
                log.error("persist file " + fileName + " exception", e);
            }
        }
    }

    public void decode(String jsonString) {
        if (jsonString != null) {
            ConsumeManager obj = JacksonUtil.INSTANCE.fromJson(jsonString, ConsumeManager.class);
            if (obj != null) {
                this.offsetTable = obj.offsetTable;
            }
        }
    }

}
