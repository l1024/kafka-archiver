package kafka.s3.consumer;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import kafka.message.MessageAndOffset;


public class App {

  private static Configuration conf;
  private static ExecutorService pool;

  /*
  mvn exec:java -Dexec.mainClass="kafka.s3.consumer.App" -Dexec.args="app.properties"
   */
  public static void main( String[] args ) throws IOException, java.lang.InterruptedException {

    conf = loadConfiguration(args);

    Map<String, Integer> topics = conf.getTopicsAndPartitions();

    List<Worker> workers = new LinkedList<Worker>();

    for (String topic: topics.keySet()) {
      for (int partition=0; partition<topics.get(topic); partition++) {
        workers.add(new Worker(topic, partition));
      }
    }

    pool = Executors.newFixedThreadPool(workers.size());

    for (Worker worker: workers) {
      pool.submit(worker);
    }
  }

  private static class Worker implements Runnable {

    private final String topic;
    private final int partition;

    private Worker(String topic, int partition) {
      this.topic = topic;
      this.partition = partition;
    }

    @Override
    public void run() {

      try {
        Sink sink = new S3Sink(topic,partition,conf);
        long offset = sink.getMaxCommittedOffset();
        Iterator<MessageAndOffset> messages = new MessageStream(topic,partition,offset,conf);
        while (messages.hasNext()) {
          MessageAndOffset messageAndOffset = messages.next();
          sink.append(messageAndOffset);
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  private static Configuration loadConfiguration(String[] args) {
    Properties props = new Properties();

    try {
      if (args == null || args.length != 1) {
        props.load(App.class.getResourceAsStream("/app.properties"));
      } else {
        props.load(new FileInputStream(new File(args[0])));
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return new PropertyConfiguration(props);
  }
}
