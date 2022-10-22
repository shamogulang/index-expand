package cn.oddworld.schedule;

import cn.oddworld.consume.ConsumeManager;
import cn.oddworld.store.ConsumeMapFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Timer;
import java.util.TimerTask;
@Component
public class ConsumerSchedule {

    private Logger log = LoggerFactory.getLogger(ConsumerSchedule.class);

    private final Timer timer = new Timer("consumeTask", false);

    @Autowired
    private ConsumeManager consumeManager;

    public void persistConsumeOffset(){
        this.timer.scheduleAtFixedRate(new TimerTask() {

            @Override
            public void run() {
                try {
                    consumeManager.persist();
                } catch (Throwable e) {
                    log.error("consume task exception", e);
                }
            }
        }, 1000 * 3, 1000);
    }
}
