package org.l1024.kafka.archiver;

/**
 * Created with IntelliJ IDEA.
 * User: lorenz
 * Date: 8/13/12
 * Time: 10:11 AM
 * To change this template use File | Settings | File Templates.
 */
public class Partition {

    private String topic;
    private int brokerId;
    private int partitionId;

    public Partition(String topic, int brokerId, int partition) {
        this.topic = topic;
        this.brokerId = brokerId;
        this.partitionId = partition;
    }

    public String getTopic() {
        return topic;
    }

    public int getBrokerId() {
        return brokerId;
    }

    public int getPartitionId() {
        return partitionId;
    }

    @Override
    public String toString() {
        return String.format("Partition(topic=%s,brokerId=%d,partitionId=%d)", topic, brokerId, partitionId);
    }
}
