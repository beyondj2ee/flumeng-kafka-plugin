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

package org.apache.flume.plugins.test;

import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.channel.PseudoTxnMemoryChannel;
import org.apache.flume.conf.Configurables;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.lifecycle.LifecycleException;
import org.apache.flume.plugins.KafkaFlumeConstans;
import org.apache.flume.plugins.KafkaSink;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Kafka Test for Sink.
 * User: beyondj2ee.
 * Date: 13. 9. 5 Time: PM 1:44.
 */
public class KafkaSinkTest {
    // - [ constant fields ] ----------------------------------------

    /**
     * The constant logger.
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaSinkTest.class);

    // - [ variable fields ] ----------------------------------------
    /**
     * The Kafka sink.
     */
    private KafkaSink sink;
    /**
     * The Flume Context.
     */
    private Context context;
    /**
     * The Max consumer thread.
     */
    private String maxConsumerThread;

    // - [ interface methods ] ------------------------------------
    // - [ protected methods ] --------------------------------------
    // - [ public methods ] -----------------------------------------

    /**
     * Sets up.
     * 
     * @throws Exception
     *             the exception
     */
    @Before
    public void setUp() throws Exception {

        // sharding partition key
        String partitionKey = "";

        this.context = new Context();
        // only kafka producer option
        this.context.put("metadata.broker.list", JunitConstans.KAFKA_SERVER);
        this.context.put("serializer.class", "kafka.serializer.StringEncoder");
        this.context.put("partitioner.class", "org.apache.flume.plugins.SinglePartition");
        this.context.put("request.required.acks", "0");
        this.context.put("max.message.size", "1000000");
        this.context.put("producer.type", "sync");

        // custom kafka producer option
        this.context.put(KafkaFlumeConstans.CUSTOME_TOPIC_KEY_NAME, JunitConstans.TOPIC_NAME);
        this.context.put(KafkaFlumeConstans.PARTITION_KEY_NAME, partitionKey);
        this.context.put(KafkaFlumeConstans.DEFAULT_ENCODING, "UTF-8");

        // create KafkaSink instance.
        sink = new KafkaSink();
        sink.setChannel(new MemoryChannel());
        Configurables.configure(sink, this.context);


    }


    /**
     * Test Sink lifecycle.
     * 
     * @throws InterruptedException
     *             the interrupted exception
     * @throws InterruptedException
     *             the interrupted exception
     */
    @Test
    public void testLifecycle() throws InterruptedException, LifecycleException {
        sink.start();
        sink.stop();
    }

    /**
     * Test produce & consume.
     * 
     * @throws InterruptedException
     *             the interrupted exception
     */
    @Test
    public void testAppend() throws Exception {

        Channel channel = new PseudoTxnMemoryChannel();
        Configurables.configure(channel, this.context);
        ConsumerChecker consumerChecker = null;

        try {

            LOGGER.info("Begin Seding message to Kafka................");

            sink.setChannel(channel);
            sink.start();
            // produce 10 messages
            for (int i = 0; i < 10; i++) {
                Event event = new SimpleEvent();
                event.setBody(("Hello Kafka :) [" + i + "]").getBytes());
                // push channel
                channel.put(event);
                sink.process();
                Thread.sleep(500);
            }
            sink.stop();

            LOGGER.info("Message Completed................");

            LOGGER.info("Begin Receiving message from Kafka................");

            Thread.sleep(5000);
            // check message count
            consumerChecker = new ConsumerChecker(JunitConstans.ZOOKEEPER_SERVER, JunitConstans.GROUP_ID,
                    JunitConstans.TOPIC_NAME, this.maxConsumerThread);
            consumerChecker.consumeLog();
            Thread.sleep(5000);
        } finally {
            if (consumerChecker != null) {
                consumerChecker.shutdown();
            }
        }
    }

    // - [ private methods ] ----------------------------------------
    // - [ static methods ] -----------------------------------------
    // - [ getter/setter methods ] ----------------------------------
    // - [ main methods ] -------------------------------------------
}
