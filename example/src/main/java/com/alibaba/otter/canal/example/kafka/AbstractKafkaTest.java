package com.alibaba.otter.canal.example.kafka;

import com.alibaba.otter.canal.example.BaseCanalClientTest;

/**
 * Kafka 测试基类
 *
 * @author machengyuan @ 2018-6-12
 * @version 1.0.0
 */
public abstract class AbstractKafkaTest extends BaseCanalClientTest {

    public static String  topic     = "cluster_mq_test";
    public static Integer partition = null;
    public static String  groupId   = "g4";
    public static String  servers   = "192.168.21.103:9092";
    public static String  zkServers = "192.168.21.103:2181";

    public void sleep(long time) {
        try {
            Thread.sleep(time);
        } catch (InterruptedException e) {
        }
    }
}
