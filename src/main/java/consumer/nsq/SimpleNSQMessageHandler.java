package consumer.nsq;

import consumer.nsq.client.NSQReceiver;

/**
 * Created by lin on 17/2/10.
 */
public class SimpleNSQMessageHandler extends NSQMessageHandler {
    public SimpleNSQMessageHandler(NSQReceiver receiver) {
        super(receiver);
    }

    public SimpleNSQMessageHandler() {
        super();
    }
}
