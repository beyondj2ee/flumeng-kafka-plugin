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

/**
 * Flume Kafka Constans.
 * User: beyondj2ee
 * Date: 13. 9. 9
 * Time: AM 11:05
 */
public class KafkaFlumeConstans {

    /**
     * The constant PARTITION_KEY_NAME.
     */
    public static final String PARTITION_KEY_NAME = "custom.partition.key";
    /**
     * The constant ENCODING_KEY_NAME.
     */
    public static final String ENCODING_KEY_NAME = "custom.encoding";
    /**
     * The constant DEFAULT_ENCODING.
     */
    public static final String DEFAULT_ENCODING = "UTF-8";
    /**
     * The constant CUSTOME_TOPIC_KEY_NAME.
     */
    public static final String CUSTOME_TOPIC_KEY_NAME = "custom.topic.name";

    /**
     * The constant CUSTOME_TOPIC_KEY_NAME.
     */
    public static final String CUSTOME_CONSUMER_THREAD_COUNT_KEY_NAME = "custom.thread.per.consumer";

}
