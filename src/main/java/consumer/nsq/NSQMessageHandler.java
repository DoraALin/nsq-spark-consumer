package consumer.nsq;

import com.youzan.nsq.client.MessageHandler;
import com.youzan.nsq.client.entity.NSQMessage;
import consumer.nsq.client.NSQReceiver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

/**
 * Created by lin on 17/2/4.
 */
public abstract class NSQMessageHandler implements MessageHandler, Serializable {

    private static final Logger logger = LoggerFactory.getLogger(NSQMessageHandler.class);
    private NSQReceiver _receiver;

    public NSQMessageHandler(NSQReceiver receiver) {
        this._receiver = receiver;
    }

    public NSQMessageHandler() {

    }

    public void setNSQReceiver(final NSQReceiver receiver) {
       this._receiver = receiver;
    }

    @Override
    public void process(NSQMessage nsqMessage) {
        fill(nsqMessage);
    }

    /**
     * function performing filling DStream with NSQMessage
     */
    private void fill(final NSQMessage nsqMessage) {
        //Get the present fetchSize from ZK set by PID Controller
        //Fetch messages from Kafka

        //nothing to buffer. Just add to Spark Block Manager
        try {
            synchronized (_receiver) {
                if (nsqMessage != null) {
                    _receiver.store(nsqMessage.getReadableContent());
                    logger.debug("NSQMessageHandler written message for partition to BM");
                }
            }
        } catch (Exception ex) {
            _receiver.reportError("Retry Store.", ex);
        }
    }
}
