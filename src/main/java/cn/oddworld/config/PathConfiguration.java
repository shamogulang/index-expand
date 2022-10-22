package cn.oddworld.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

import java.io.File;

@Configuration
public class PathConfiguration {

    @Value("${root.store.path}")
    private String rootPath;
    @Value("${root.store.commitFileSize:1024}")
    private int commitFileSize;
    @Value("${root.store.consumeFileSize:1024}")
    private int consumeFileSize;


    public String getConsumePath(){

        return rootPath + File.separator + "config" + File.separator + "consumerOffset.json";
    }

    public String getConsumeMapFilePath(){

        return rootPath + File.separator + "config";
    }

    public String getCommitMapFilePath(){

        return rootPath + File.separator + "commit";
    }

    public String getRootPath() {
        return rootPath;
    }

    public void setRootPath(String rootPath) {
        this.rootPath = rootPath;
    }

    public int getCommitFileSize() {
        return commitFileSize;
    }

    public void setCommitFileSize(int commitFileSize) {
        this.commitFileSize = commitFileSize;
    }

    public int getConsumeFileSize() {
        return consumeFileSize;
    }

    public void setConsumeFileSize(int consumeFileSize) {
        this.consumeFileSize = consumeFileSize;
    }
}
