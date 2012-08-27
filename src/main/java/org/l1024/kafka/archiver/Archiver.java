package org.l1024.kafka.archiver;

import kafka.message.MessageAndOffset;
import kafka.server.KafkaConfig;
import org.apache.log4j.Logger;
import org.l1024.kafka.archiver.config.Configuration;

import javax.management.*;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;


public class Archiver implements ArchiverMBean {

    private static final Logger logger = Logger.getLogger(Archiver.class);

    private KafkaConfig kafkaConfig;
    private Configuration configuration;

    List<ThreadContainer> threadContainers = new LinkedList<ThreadContainer>();

    private long threadRestarts = 0L;

    public Archiver(KafkaConfig kafkaConfig, Configuration configuration) {
        this.kafkaConfig = kafkaConfig;
        this.configuration = configuration;
    }

    public void start() throws IOException, InterruptedException {
        logger.info(String.format(
                "Starting workers to archive kafka topics:\n"
                        + "  brokerId:       %d\n"
                        + "  bucket/key:     %s/%s\n"
                        + "  topics:         %s\n"
                        + "  chunkSize:      %d\n"
                        + "  commitInterval: %d",
                kafkaConfig.brokerId(),
                configuration.getS3Bucket(), configuration.getS3Prefix(),
                configuration.getTopics(),
                configuration.getMinTotalMessageSizePerChunk(),
                configuration.getMaxCommitInterval()));

        int brokerId = kafkaConfig.brokerId();
        String kafkaHost;
        int kafkaPort;

        if (kafkaConfig.hostName() == null) {
            kafkaHost = "localhost";
        } else {
            kafkaHost = kafkaConfig.hostName();
        }
        if (kafkaConfig.port() == 0) {
            kafkaPort = 9092;
        } else {
            kafkaPort = kafkaConfig.port();
        }

        MessageStreamFactory messageStreamFactory =
                new MessageStreamFactory(
                        kafkaHost,
                        kafkaPort,
                        configuration.getKafkaMaxMessageSize()
                );

        SinkFactory sinkFactory =
                new SinkFactory(
                        configuration.getS3AccessKey(),
                        configuration.getS3SecretKey(),
                        configuration.getS3Bucket(),
                        configuration.getS3Prefix()
                );

        WorkerFactory workerFactory = new WorkerFactory(messageStreamFactory, sinkFactory, configuration.getMinTotalMessageSizePerChunk(), configuration.getMaxCommitInterval());
        threadContainers = new LinkedList<ThreadContainer>();

        for (String topic : configuration.getTopics()) {
            int numPartitions;
            if (kafkaConfig.topicPartitionsMap().contains(topic)) {
                numPartitions = (Integer)kafkaConfig.topicPartitionsMap().apply(topic);
            } else {
                numPartitions = kafkaConfig.numPartitions();
            }
            for (int partitionId = 0; partitionId < numPartitions; partitionId++) {
                Partition partition = new Partition(topic, brokerId, partitionId);
                logger.info(String.format("Creating consumer for %s", partition));
                threadContainers.add(new ThreadContainer(partition, workerFactory));
            }
        }

        for (ThreadContainer threadContainer : threadContainers) {
            threadContainer.start();
        }

        while (true) {
            Thread.sleep(60000);
            for (ThreadContainer threadContainer : threadContainers) {
                if (threadContainer.maintain()) {
                    threadRestarts += 1;
                }
            }
        }

    }

    private static class ThreadContainer {

        private Partition partition;
        private WorkerFactory workerFactory;

        private ArchivingWorker worker;
        private Thread thread;

        public ThreadContainer(Partition partition, WorkerFactory workerFactory) {
            this.partition = partition;
            this.workerFactory = workerFactory;
        }

        public void start() throws IOException {
            logger.info(String.format("Creating worker. (%s)", partition));
            createThread();
            logger.debug(worker);
        }

        public boolean maintain() throws IOException {
            boolean restart = !thread.isAlive() && !worker.isFinished();
            if (restart) {
                logger.info(String.format("Thread (%s) died. Recreating worker. (%s)", thread, partition));
                createThread();
            }
            logger.debug(worker);
            return restart;
        }

        private void createThread() {
            worker = workerFactory.createWorker(partition);
            thread = new Thread(worker);
            thread.setUncaughtExceptionHandler(new ExceptionHandler());
            thread.start();
        }

        @Override
        public String toString() {
            return String.format("ThreadContainer(partition=%s,worker=%s)", partition, worker);
        }

        private static class ExceptionHandler implements Thread.UncaughtExceptionHandler {

            @Override
            public void uncaughtException(Thread t, Throwable e) {
                logger.error(e);
            }
        }

    }

    private static class ArchivingWorker implements Runnable {

        private Partition partition;
        private MessageStreamFactory messageStreamFactory;
        private SinkFactory sinkFactory;
        long minTotalMessageSizePerChunk;
        int maxCommitInterval;
        private boolean finished = false;

        private Sink sink;
        private MessageStream messages;

        private ArchivingWorker(Partition partition, MessageStreamFactory messageStreamFactory, SinkFactory sinkFactory, long minTotalMessageSizePerChunk, int maxCommitInterval) {
            this.partition = partition;
            this.messageStreamFactory = messageStreamFactory;
            this.sinkFactory = sinkFactory;
            this.minTotalMessageSizePerChunk = minTotalMessageSizePerChunk;
            this.maxCommitInterval = maxCommitInterval;
        }

        @Override
        public void run() {

            try {

                sink = sinkFactory.createSink(partition);
                messages = messageStreamFactory.createMessageStream(partition, sink.getMaxCommittedOffset());

                logger.info(String.format("Worker starts archiving messages from partition %s starting with offset %d", partition, sink.getMaxCommittedOffset()));

                while (messages.hasNext()) {
                    MessageAndOffset messageAndOffset = messages.next((sink.getLastCommitTimestamp() + maxCommitInterval) - System.currentTimeMillis());
                    if (messageAndOffset != null) {
                        sink.append(messageAndOffset);
                    }
                    if (sink.getUncommittedMessageSize() > minTotalMessageSizePerChunk) {
                        logger.info(String.format("Committing chunk for %s. (size)", partition));
                        sink.commitChunk();
                    } else if (System.currentTimeMillis() - sink.getLastCommitTimestamp() > maxCommitInterval) {
                        logger.info(String.format("Committing chunk for %s. (interval)", partition));
                        sink.commitChunk();
                    }
                }

                finished = true;

                logger.info(toString() + " finished.");
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        public boolean isFinished() {
            return finished;
        }

        @Override
        public String toString() {
            if (messages == null || sink == null) {
                return String.format("ArchivingWorker(uninitialized)");
            } else {
                return String.format("ArchivingWorker(messages=%s,sink=%s)", messages.toString(), sink.toString());
            }
        }
    }

    private static class WorkerFactory {

        private MessageStreamFactory messageStreamFactory;
        private SinkFactory sinkFactory;
        private long minTotalMessageSizePerChunk;
        private int maxCommitInterval;

        private WorkerFactory(MessageStreamFactory messageStreamFactory, SinkFactory sinkFactory, long minTotalMessageSizePerChunk, int maxCommitInterval) {
            this.messageStreamFactory = messageStreamFactory;
            this.sinkFactory = sinkFactory;
            this.minTotalMessageSizePerChunk = minTotalMessageSizePerChunk;
            this.maxCommitInterval = maxCommitInterval;
        }

        public ArchivingWorker createWorker(Partition partition) {

            return new ArchivingWorker(partition, messageStreamFactory, sinkFactory, minTotalMessageSizePerChunk, maxCommitInterval);
        }
    }

    @Override
    public String toString() {
        return String.format("Archiver(threadContainers=%s)", threadContainers);
    }

    // JMX MBean methods
    @Override
    public Set<String> getTopics() {
        return configuration.getTopics();
    }

    @Override
    public long getThreadRestarts() {
        return threadRestarts;
    }

    @Override
    public int getTotalPartitions() {
        return threadContainers.size();
    }

    public static void main(String[] args) throws IOException, InterruptedException, MalformedObjectNameException, MBeanRegistrationException, InstanceAlreadyExistsException, NotCompliantMBeanException {

        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();

        if (args.length != 2) {
            System.out.println("Usage: java -jar <kafka consumer jar> <server properties> <archiver properties>");
        }
        KafkaConfig kafkaConfig = Configuration.loadKafkaConfiguration(args[0]);
        Configuration configuration = Configuration.loadConfiguration(args[1]);

        Archiver archiver = new Archiver(kafkaConfig, configuration);

        mbs.registerMBean(archiver, new ObjectName("Archiver:type=org.l1024.kafka.archiver.Archiver"));

        archiver.start();
    }

}

