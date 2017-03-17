package consumer.nsq;

import com.youzan.nsq.client.Consumer;
import com.youzan.nsq.client.ConsumerImplV2;
import com.youzan.nsq.client.entity.NSQConfig;
import com.youzan.nsq.client.entity.Topic;
import com.youzan.nsq.client.exception.NSQException;
import consumer.nsq.client.NSQReceiver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

/**
 * NSQConsumer subscribe to NSQ broker, as NSQ is in push mode MQ. this class is NOT a {@link Runnable}.
 * Created by lin on 17/2/4.
 */
@SuppressWarnings("serial")
public class NSQConsumer implements Serializable, AutoCloseable{

    private final static Logger logger = LoggerFactory.getLogger(NSQConsumer.class);

    private Consumer _consumer;
    private NSQConsumerConfig _config;
    private Topic _topic;
    private NSQReceiver _receiver;
    private final NSQMessageHandler _messageHandler;

    public NSQConsumer(
            NSQConsumerConfig config,
            NSQReceiver receiver,
            NSQMessageHandler messageHandler) {
        _config = config;
        _topic = new Topic(_config._stateConf.get("nsq.topic"));
        _receiver = receiver;
        _messageHandler = messageHandler;
    }

    /**
     * Initialize and start {@link Consumer} to NSQ broker
     * @throws NSQException exception when NSQConsumer fail to start.
     */
    public void open() throws NSQException {
        if(null != _consumer)
            return;
        //intialize {@link NSQConfig} out of NSQConsumerConfig
        NSQConfig config = _config.initNSQConfig();
        _consumer = new ConsumerImplV2(config, _messageHandler);
        _consumer.subscribe(_topic);
        try {
            _consumer.start();
        } catch (NSQException e) {
            logger.error("Could not start NSQ inner consumer.");
            throw e;
        }
    }

    @Override
    public void close() throws Exception {
        if(null != _consumer)
            _consumer.close();
    }
}
