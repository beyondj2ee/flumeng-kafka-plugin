/*
 *  Copyright (c) 2013.09.06 BeyondJ2EE.
 *  * All right reserved.
 *  * http://beyondj2ee.github.com
 *  * This software is the confidential and proprietary information of BeyondJ2EE
 *  * , Inc. You shall not disclose such Confidential Information and
 *  * shall use it only in accordance with the terms of the license agreement
 *  * you entered into with BeyondJ2EE.
 *  *
 *  * Revision History
 *  * Author              Date                  Description
 *  * ===============    ================       ======================================
 *  *  beyondj2ee
 *
 */

package org.apache.flume.plugins;

import java.lang.management.ManagementFactory;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.instrumentation.SourceCounter;
import org.apache.flume.source.AbstractSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableMap;

import javax.management.ObjectName;

/**
 * Kafka Flume Source (Kafka 0.8 Beta, Flume 1.4). User: beyondj2ee Date: 13. 9. 9 Time: AM 9:56
 */
public class KafkaSource extends AbstractSource implements EventDrivenSource, Configurable {
    // - [ constant fields ] ----------------------------------------
    /**
     * The constant logger.
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaSink.class);

    // - [ variable fields ] ----------------------------------------
    /**
     * The Parameters.
     */
    private Properties parameters;
    /**
     * The Context.
     */
    private Context context;
    /**
     * The Consumer connector.
     */
    private ConsumerConnector consumerConnector;
    /**
     * The Executor service.
     */
    private ExecutorService executorService;

    /**
     * The Source counter.
     */
    private SourceCounter sourceCounter;

    // - [ interface methods ] ------------------------------------

    /**
     * Configure void.
     *
     * @param context the context
     */
    @Override
    public void configure(Context context) {
        this.context = context;
        ImmutableMap<String, String> props = context.getParameters();

        this.parameters = new Properties();
        for (String key : props.keySet()) {
            String value = props.get(key);
            this.parameters.put(key, value);
        }

        //source monitoring count
        if (sourceCounter == null) {
            sourceCounter = new SourceCounter(getName());
        }
    }

    /**
     * Start void.
     */
    @Override
    public synchronized void start() {

        super.start();
        sourceCounter.start();
        LOGGER.info("Kafka Source started...");

        // make config object
        ConsumerConfig consumerConfig = new ConsumerConfig(this.parameters);
        consumerConnector = kafka.consumer.Consumer.createJavaConsumerConnector(consumerConfig);

        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();

        String topic = (String) this.parameters.get(KafkaFlumeConstans.CUSTOME_TOPIC_KEY_NAME);
        String threadCount = (String) this.parameters.get(KafkaFlumeConstans.CUSTOME_CONSUMER_THREAD_COUNT_KEY_NAME);

        topicCountMap.put(topic, new Integer(threadCount));

        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumerConnector
                .createMessageStreams(topicCountMap);

        List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);

        // now launch all the threads
        this.executorService = Executors.newFixedThreadPool(Integer.parseInt(threadCount));

        // now create an object to consume the messages
        int tNumber = 0;
        for (final KafkaStream stream : streams) {
            this.executorService.submit(new ConsumerWorker(stream, tNumber, sourceCounter));
            tNumber++;
        }
    }

    /**
     * Stop void.
     */
    @Override
    public synchronized void stop() {
        try {
            shutdown();
        } catch (Exception e) {
            e.printStackTrace();
        }
        super.stop();
        sourceCounter.stop();

        // Remove the MBean registered for Monitoring
        ObjectName objName = null;
        try {
            objName = new ObjectName("org.apache.flume.source"
                    + ":type=" + getName());

            ManagementFactory.getPlatformMBeanServer().unregisterMBean(objName);
        } catch (Exception ex) {
            System.out.println("Failed to unregister the monitored counter: "
                    + objName + ex.getMessage());
        }
    }

    // - [ public methods ] -----------------------------------------
    // - [ private methods ] ----------------------------------------

    /**
     * shutdown consumer threads.
     * @throws Exception the exception
     */
    private void shutdown() throws Exception {
        if (consumerConnector != null) {
            consumerConnector.shutdown();
        }

        if (executorService != null) {
            executorService.shutdown();
        }

        executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
    }

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

        private SourceCounter srcCount;

        /**
         * Instantiates a new Consumer test.
         *
         * @param kafkaStream the kafka stream
         * @param threadNumber the thread number
         */
        public ConsumerWorker(KafkaStream kafkaStream, int threadNumber, SourceCounter srcCount) {
            this.kafkaStream = kafkaStream;
            this.threadNumber = threadNumber;
            this.srcCount = srcCount;
        }

        /**
         * Run void.
         */
        public void run() {

            ConsumerIterator<byte[], byte[]> it = this.kafkaStream.iterator();
            try {
                while (it.hasNext()) {

                    //get message from kafka totpic
                    byte [] message = it.next().message();

                    LOGGER.info("Receive Message [Thread " + this.threadNumber + ": " + new String(message,"UTF-8") + "]");
                    //create event
                    Event event = EventBuilder.withBody(message);
                    //send event to channel
                    getChannelProcessor().processEvent(event);
                    this.srcCount.incrementEventAcceptedCount();
                }
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
    }
    // - [ main methods ] -------------------------------------------
}
