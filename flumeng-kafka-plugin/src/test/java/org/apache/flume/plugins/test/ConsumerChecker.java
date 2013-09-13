package org.apache.flume.plugins.test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Kafka Message Checker. User: beyondj2ee Date: 13. 9. 6 Time: PM 5:23
 */
public class ConsumerChecker {
    // - [ constant fields ] ----------------------------------------

    /**
     * The constant LOGGER.
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(ConsumerChecker.class);

    /**
     * The constant maxThreads.
     */
    // - [ variable fields ] ----------------------------------------
    /**
     * The Consumer connector.
     */
    private ConsumerConnector consumerConnector;
    /**
     * The Executor service.
     */
    private ExecutorService executorService;

    /**
     * Instantiates a new Consumer checker.
     * 
     * @param zookeeper
     *            the zookeeper
     * @param groupId
     *            the group id
     * @param topic
     *            the topic
     * @param threadNumber
     *            the thread number
     */
    public ConsumerChecker(String zookeeper, String groupId, String topic, String threadNumber) {

        Properties props = new Properties();
        props.put("zookeeper.connect", zookeeper);
        props.put("group.id", groupId);
        props.put("zookeeper.session.timeout.ms", "400");
        props.put("zookeeper.sync.time.ms", "200");
        props.put("auto.commit.interval.ms", "1000");

        ConsumerConfig consumerConfig = new ConsumerConfig(props);
        consumerConnector = kafka.consumer.Consumer.createJavaConsumerConnector(consumerConfig);
    }

    // - [ construction methods ] ---------------------------------

    /**
     * Shutdown void.
     */
    public void shutdown() {
        if (consumerConnector != null) {
            consumerConnector.shutdown();
        }

        if (executorService != null) {
            executorService.shutdown();
        }
    }

    /**
     * Gets consume count.
     * 
     * @return the consume count
     */
    public void consumeLog() throws Exception {

        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(JunitConstans.TOPIC_NAME, new Integer(JunitConstans.MAX_CONSUMER_COUNT));

        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumerConnector
                .createMessageStreams(topicCountMap);

        List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(JunitConstans.TOPIC_NAME);

        // now launch all the threads
        this.executorService = Executors.newFixedThreadPool(Integer.parseInt(JunitConstans.MAX_CONSUMER_COUNT));

        // now create an object to consume the messages
        int tNumber = 0;
        for (final KafkaStream stream : streams) {
            this.executorService.submit(new ConsumerWorker(stream, tNumber));
            tNumber++;
        }
    }

    // - [ interface methods ] ------------------------------------
    // - [ protected methods ] --------------------------------------
    // - [ public methods ] -----------------------------------------
    // - [ private methods ] ----------------------------------------
    // - [ static methods ] -----------------------------------------
    // - [ getter/setter methods ] ----------------------------------

    /**
     * Real Consumer Thread.
     */
    private class ConsumerWorker implements Runnable {

        /**
         * The M _ stream.
         */
        private KafkaStream kafkaStream;
        /**
         * The M _ thread number.
         */
        private int threadNumber;

        /**
         * Instantiates a new Consumer test.
         * 
         * @param kafkaStream
         *            the kafka stream
         * @param threadNumber
         *            the thread number
         */
        public ConsumerWorker(KafkaStream kafkaStream, int threadNumber) {
            this.kafkaStream = kafkaStream;
            this.threadNumber = threadNumber;
        }

        /**
         * Run void.
         */
        public void run() {
            ConsumerIterator<byte[], byte[]> it = this.kafkaStream.iterator();
            while (it.hasNext()) {
                LOGGER.info("Receive Message [Thread " + this.threadNumber + ": " + new String(it.next().message())
                        + "]");
            }
        }
    }

    // - [ main methods ] -------------------------------------------
}
