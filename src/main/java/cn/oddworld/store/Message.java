package cn.oddworld.store;

import java.util.Map;

public class Message {

    private Map<String, String> properties;
    private String business;
    private byte[] body;

    public Message() {
    }

    public Message(byte[] body, String business) {
        this.body = body;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }

    public String getBusiness() {
        return business;
    }

    public void setBusiness(String business) {
        this.business = business;
    }

    public byte[] getBody() {
        return body;
    }

    public void setBody(byte[] body) {
        this.body = body;
    }
}
