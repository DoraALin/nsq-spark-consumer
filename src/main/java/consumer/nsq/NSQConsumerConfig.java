package consumer.nsq;

import com.youzan.nsq.client.entity.NSQConfig;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Created by lin on 17/2/6.
 */
@SuppressWarnings("serial")
public class NSQConsumerConfig implements Serializable {

    public Map<String,String> _stateConf;
    //Number of fetch consumer will buffer before writing to Spark Block Manager
    public int _numFetchToBuffer = 1;

    //Parameters for Controllers

    public NSQConsumerConfig(Properties props) {

        //NSQ Topic
        String nsqTopic = props.getProperty(Config.NSQ_CONSUMER_TOPIC);
        String consumerName = props.getProperty(Config.NSQ_CONSUMER_NAME);

        _stateConf = new HashMap<>();
        _stateConf.put(Config.NSQ_CONSUMER_TOPIC, nsqTopic);
        _stateConf.put(Config.NSQ_CONSUMER_NAME, consumerName);
    }

    /**
     *Initialize {@link com.youzan.nsq.client.entity.NSQConfig} out of current consumer config
     */
    public NSQConfig initNSQConfig(){
        NSQConfig config = new NSQConfig(_stateConf.get(Config.NSQ_CONSUMER_NAME));
        return config;
    }
}
