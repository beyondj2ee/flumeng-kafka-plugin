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

import com.google.common.base.Preconditions;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.apache.commons.lang.StringUtils;
import org.apache.flume.*;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.channel.PseudoTxnMemoryChannel;
import org.apache.flume.channel.ReplicatingChannelSelector;
import org.apache.flume.conf.Configurables;
import org.apache.flume.lifecycle.LifecycleException;
import org.apache.flume.plugins.KafkaFlumeConstans;
import org.apache.flume.plugins.KafkaSink;
import org.apache.flume.plugins.KafkaSource;
import org.apache.flume.source.AbstractSource;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import java.util.Properties;

/**
 * Kafka Test for Source. User: Administrator Date: 13. 9. 9 Time: AM 10:47
 */
public class KafkaSourceTest {

    /**
     * The constant LOGGER.
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaSink.class);
    /**
     * The Source.
     */
    private AbstractSource source;
    /**
     * The Channel.
     */
    private Channel channel = new MemoryChannel();
    /**
     * The Context.
     */
    private Context context = new Context();
    /**
     * The Rcs.
     */
    private ChannelSelector rcs = new ReplicatingChannelSelector();

    /**
     * Sets up.
     *
     * @throws Exception              the exception
     */
    @Before
    public void setUp() throws Exception {

        // kafka config
        this.context.put("zookeeper.connect", JunitConstans.ZOOKEEPER_SERVER);
        this.context.put("group.id", JunitConstans.GROUP_ID);
        this.context.put("zookeeper.session.timeout.ms", "400");
        this.context.put("zookeeper.sync.time.ms", "200");
        this.context.put("auto.commit.interval.ms", "1000");

        // custom config
        this.context.put(KafkaFlumeConstans.CUSTOME_TOPIC_KEY_NAME, JunitConstans.TOPIC_NAME);
        this.context.put(KafkaFlumeConstans.DEFAULT_ENCODING, "UTF-8");
        this.context.put(KafkaFlumeConstans.CUSTOME_CONSUMER_THREAD_COUNT_KEY_NAME, "4");

        Configurables.configure(this.channel, this.context);
        rcs.setChannels(Lists.newArrayList(this.channel));

        this.source = new KafkaSource();
        this.source.setChannelProcessor(new ChannelProcessor(rcs));
        Configurables.configure(this.source, this.context);

    }

    /**
     * Test lifecycle.
     *
     * @throws InterruptedException              the interrupted exception
     * @throws InterruptedException              the interrupted exception
     */
    @Test
    public void testLifecycle() throws InterruptedException, LifecycleException {
        source.start();
        source.stop();
    }


    /**
     * Test append.
     *
     * @throws Exception              the exception
     */
    @Test
    public void testAppend() throws Exception {
        LOGGER.info("Begin Seding message to Kafka................");
        source.start();

        //send message
        sendMessageToKafka();

        Thread.sleep(5000);

        //get message from channel
        Transaction transaction = channel.getTransaction();

        try{

            transaction.begin();
            Event event;
            while ((event = channel.take()) != null) {
                LOGGER.info("#get channel########" + new String(event.getBody(), "UTF-8"));
            }
            transaction.commit();
        } finally{
            transaction.close();
        }
    }

    /**
     * Send message to kafka.
     */
    private void sendMessageToKafka() {


        Properties parameters = new Properties();

        parameters.put("metadata.broker.list", JunitConstans.KAFKA_SERVER);
        parameters.put("serializer.class", "kafka.serializer.StringEncoder");
        parameters.put("partitioner.class", "org.apache.flume.plugins.SinglePartition");
        parameters.put("request.required.acks", "0");
        parameters.put("max.message.size", "1000000");
        parameters.put("producer.type", "sync");

        ProducerConfig config = new ProducerConfig(parameters);
        Producer producer = new Producer<String, String>(config);
        String encoding = KafkaFlumeConstans.DEFAULT_ENCODING;
        String topic = JunitConstans.TOPIC_NAME;
        KeyedMessage<String, String> data;

        for (int i=0; i< 10; i++) {
            data = new KeyedMessage<String, String>(topic, "send data : message " + i +"]");
            producer.send(data);
        }
    }
}
