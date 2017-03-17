package consumer.nsq;

import consumer.nsq.client.NSQReceiver;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.StreamingContext;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.dstream.DStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * ReceiverLauncher for starting {@link consumer.nsq.client.NSQReceiver}
 * Created by lin on 17/2/7.
 */
@SuppressWarnings("serial")
public class NSQReceiverLauncher implements Serializable {
    public static final Logger LOG = LoggerFactory.getLogger(NSQReceiverLauncher.class);

    public static DStream<String> launch(
            StreamingContext ssc, Properties pros,
            StorageLevel storageLevel, NSQMessageHandler messageHandler) {
        JavaStreamingContext jsc = new JavaStreamingContext(ssc);
        return createStream(jsc, pros, storageLevel, messageHandler).dstream();
    }

    public static JavaDStream<String> launch(
            JavaStreamingContext jsc, Properties pros,
            StorageLevel storageLevel) {
        return createStream(jsc, pros, storageLevel, new SimpleNSQMessageHandler());
    }

    private static JavaDStream<String> createStream(
            JavaStreamingContext jsc, Properties pros, StorageLevel storageLevel,
            NSQMessageHandler messageHandler) {

        List<JavaDStream<String>> streamsList = new ArrayList<>();
        JavaDStream<String> unionStreams;

        // Create as many Receiver as Partition
        streamsList.add(jsc.receiverStream(new NSQReceiver(
            pros, storageLevel, messageHandler)));

        // Union all the streams if there is more than 1 stream
        if (streamsList.size() > 1) {
            unionStreams =
                    jsc.union(
                            streamsList.get(0), streamsList.subList(1, streamsList.size()));
        } else {
            // Otherwise, just use the 1 stream
            unionStreams = streamsList.get(0);
        }

        return unionStreams;
    }
}

