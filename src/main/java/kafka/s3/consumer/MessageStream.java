package kafka.s3.consumer;

import java.util.Iterator;

import kafka.api.FetchRequest;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.MessageSet;
import kafka.message.MessageAndOffset;

import org.apache.log4j.Logger;

class MessageStream implements Iterator<MessageAndOffset> {

    private static final Logger logger = Logger.getLogger(MessageStream.class);

    private Configuration conf;

    private SimpleConsumer consumer;
    private Iterator<MessageAndOffset> messageSetIterator;

    private String topic;
    private int partition;
    private long offset;

    public MessageStream(String topic, int partition, long offset, Configuration conf) {

      this.conf = conf;

      logger.debug("Message stream created: " + topic + ":" + partition + "/" + offset);
      this.topic = topic;
      this.partition = partition;
      this.offset = offset;
      consumer = new SimpleConsumer(conf.getKafkaHost(), conf.getKafkaPort(), 5000, 4*1024);
      logger.debug("Created kafka consumer: " + consumer);
    }

    @Override
    public boolean hasNext() {
      return true;
    }

    @Override
    public MessageAndOffset next() {
      if (messageSetIterator == null || !messageSetIterator.hasNext()) {
        logger.debug("Fetching message from offset: " + offset);
        FetchRequest fetchRequest = new FetchRequest(topic, partition, offset, conf.getKafkaMaxMessageSize());
        MessageSet messageSet = consumer.fetch(fetchRequest);
        while (!messageSet.iterator().hasNext()) {
          logger.debug("No messages returned. Sleeping for 10s.");
          try {
            Thread.sleep(10000);
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
          messageSet = consumer.fetch(fetchRequest);
        }
        messageSetIterator = messageSet.iterator();
      }
      MessageAndOffset message = messageSetIterator.next();
      offset = message.offset();
      return message;
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException("Method remove is not supported by this iterator.");
    }
  }