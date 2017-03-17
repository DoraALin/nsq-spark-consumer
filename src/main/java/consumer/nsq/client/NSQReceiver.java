package consumer.nsq.client;

import com.youzan.nsq.client.exception.NSQException;
import consumer.nsq.NSQConsumer;
import consumer.nsq.NSQConsumerConfig;
import consumer.nsq.NSQMessageHandler;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Created by lin on 17/2/4.
 */
public class NSQReceiver extends Receiver<String> {

    private static final Logger logger = LoggerFactory.getLogger(NSQReceiver.class);
    private final Properties _props;
    private NSQConsumer _consumer;
    private NSQMessageHandler _messageHandler;

    public NSQReceiver(Properties props,
                       NSQMessageHandler messageHandler){
        this(props, StorageLevel.MEMORY_ONLY(), messageHandler);
    }

    public NSQReceiver(
            Properties props,
            StorageLevel storageLevel,
            NSQMessageHandler messageHandler) {
        super(storageLevel);
        this._props = props;
        this._messageHandler = messageHandler;
        this._messageHandler.setNSQReceiver(this);
    }

    @Override
    public void onStart() {
        start();
    }

    public void start() {
        // Start the thread that receives data over a connection
        //initialize NSQConfig via proeprties.
        NSQConsumerConfig nsqConfig = new NSQConsumerConfig(_props);

        //TODO: create consumer to NSQ broker.
        try {
            _consumer = new NSQConsumer(nsqConfig, this, _messageHandler);
            _consumer.open();
        }catch (NSQException e){
            logger.error("Could not start NSQReceiver.", e);
        }
    }

    @Override
    public void onStop() {
        try {
            _consumer.close();
        } catch (Exception e) {
            logger.error("Error closing nsq consumer.", e);
        }
    }
}
