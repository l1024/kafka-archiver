package org.l1024.kafka.archiver;

import kafka.api.FetchRequest;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.message.MessageAndOffset;
import org.apache.log4j.Logger;

import java.util.Iterator;

class MessageStream {

    private static final Logger logger = Logger.getLogger(MessageStream.class);

    private Partition partition;
    private long offset;

    private SimpleConsumer consumer;
    private int kafkaMaxMessageSize;
    private Iterator<MessageAndOffset> messageSetIterator;

    protected MessageStream(Partition partition, long offset, SimpleConsumer consumer, int kafkaMaxMessageSize) {
        this.partition = partition;
        this.offset = offset;

        this.consumer = consumer;
        this.kafkaMaxMessageSize = kafkaMaxMessageSize;
    }

    public boolean hasNext() {
        return true;
    }

    public MessageAndOffset next(long timeOut) {

        long start = System.currentTimeMillis();

        if (messageSetIterator == null || !messageSetIterator.hasNext()) {

            logger.debug("Fetching message from offset: " + offset);

            FetchRequest fetchRequest = new FetchRequest(partition.getTopic(), partition.getPartitionId(), offset, kafkaMaxMessageSize);

            messageSetIterator = consumer.fetch(fetchRequest).iterator();

            while (!messageSetIterator.hasNext()) {
                logger.debug("No messages returned. Sleeping for 10s.");
                try {
                    Thread.sleep(10000);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                if (System.currentTimeMillis() - start > timeOut) {
                    return null;
                }
                messageSetIterator = consumer.fetch(fetchRequest).iterator();
            }
        }
        MessageAndOffset message = messageSetIterator.next();
        offset = message.offset();
        return message;
    }

    @Override
    public String toString() {
        return String.format("MessageStream(partition=%s,offset=%d)",partition,offset);
    }
  }