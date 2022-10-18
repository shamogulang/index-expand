package cn.oddworld.store;

import java.util.Iterator;
import java.util.List;
public class MessageBatch extends Message {

    private final List<Message> messages;

    private MessageBatch(List<Message> messages) {
        this.messages = messages;
    }

    public Iterator<Message> iterator() {
        return messages.iterator();
    }

    public List<Message> getMessages() {
        return messages;
    }
}
