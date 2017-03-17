package consumer.nsq.client;


import consumer.nsq.Config;
import consumer.nsq.NSQReceiverLauncher;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.io.Serializable;
import java.util.List;
import java.util.Properties;

/**
 * Created by lin on 17/2/10.
 */
@SuppressWarnings("serial")
public class NSQSampleConsumer implements Serializable {

    public void start() throws InstantiationException, IllegalAccessException,
            ClassNotFoundException {
        run();
    }

    @SuppressWarnings("deprecation")
    private void run() {

        //TODO: NSQ properties
        Properties props = new Properties();
        props.put(Config.NSQ_CONSUMER_TOPIC, "JavaTesting-Producer-Base");
        props.put(Config.NSQ_CONSUMER_NAME, "BaseConsumer");

        SparkConf _sparkConf = new SparkConf();
        _sparkConf.setAppName("NSQ-spark-consumer-example")
                .setMaster("local");
        JavaStreamingContext jsc = new JavaStreamingContext(_sparkConf, Durations.seconds(30));
        // Specify number of Receivers you need.

        //TODO: missing message handler
        JavaDStream<String> unionStreams = NSQReceiverLauncher.launch(
                jsc, props, StorageLevel.MEMORY_ONLY());

        unionStreams.foreachRDD(new VoidFunction<JavaRDD<String>>() {
            @Override
            public void call(JavaRDD<String> rdd) throws Exception {
                List<String> rddList = rdd.collect();
                System.out.println(" Number of records in this batch " + rddList.size());
                for(String msg:rddList) {
                    System.out.println(msg);
                }
            }
        });
        //End Application Logic

        try {
            jsc.start();
            jsc.awaitTermination();
        }catch (Exception ex ) {
            jsc.ssc().sc().cancelAllJobs();
            jsc.stop(true, false);
            System.exit(-1);
        }
    }

    public static void main(String[] args) throws Exception {
        NSQSampleConsumer consumer = new NSQSampleConsumer();
        consumer.start();
    }
}
