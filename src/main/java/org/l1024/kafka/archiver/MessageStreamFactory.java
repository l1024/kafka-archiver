package org.l1024.kafka.archiver;

import kafka.javaapi.consumer.SimpleConsumer;

/**
 * Created with IntelliJ IDEA.
 * User: lorenz
 * Date: 8/10/12
 * Time: 11:33 AM
 * To change this template use File | Settings | File Templates.
 */
public class MessageStreamFactory {

    private String kafkaHost;
    private int kafkaPort;
    private int maxMessageSize;

    public MessageStreamFactory(String kafkaHost, int kafkaPort, int maxMessageSize) {
        this.kafkaHost = kafkaHost;
        this.kafkaPort = kafkaPort;
        this.maxMessageSize = maxMessageSize;
    }

    public MessageStream createMessageStream(Partition partition, long offset) {

        SimpleConsumer consumer = new SimpleConsumer(kafkaHost, kafkaPort, 5000, 4*1024);

        return new MessageStream(
                partition,
                offset,
                consumer,
                maxMessageSize
        );
    }
}
