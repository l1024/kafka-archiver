package org.l1024.kafka.archiver.s3;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import kafka.message.MessageAndOffset;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.log4j.Logger;
import org.l1024.kafka.archiver.Partition;
import org.l1024.kafka.archiver.Sink;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

public class S3Sink implements Sink {

    static final Logger logger = Logger.getLogger(S3Sink.class);

    private AmazonS3Client s3Client;

    private final String bucket;
    private final String keyPrefix;

    Partition partition;
    private long maxCommittedOffset;

    private long committedMessageCount = 0;
    private long committedMessageSize = 0;

    private long lastCommitTimestamp;

    private Chunk chunk;

    public S3Sink(
            AmazonS3Client s3Client,
            String bucket,
            String keyPrefix,
            Partition partition) throws IOException {

        this.s3Client = s3Client;

        this.bucket = bucket;
        this.keyPrefix = keyPrefix + "/" + partition.getTopic() + "/" + partition.getBrokerId() + "_" + partition.getPartitionId() + "_";

        this.partition = partition;
        maxCommittedOffset = fetchLastCommittedOffset();

        lastCommitTimestamp = System.currentTimeMillis();

        chunk = Chunk.createChunk(partition, maxCommittedOffset);
    }

    @Override
    public void append(MessageAndOffset messageAndOffset) throws IOException {

        int messageSize = messageAndOffset.message().payloadSize();
        logger.debug("Appending message with size: " + messageSize);

        chunk.appendMessageToChunk(messageAndOffset);
    }

    @Override
    public void commitChunk() throws IOException {

        long startOffset = chunk.getStartOffset();
        long endOffset = chunk.getEndOffset();

        if (maxCommittedOffset != startOffset) {
            throw new RuntimeException("Gap between chunks detected.");
        }

        lastCommitTimestamp = System.currentTimeMillis();

        if (chunk.isEmpty()) {
            logger.debug("Empty chunk. Nothing to upload.");
            return;
        }

        File tmpChunkFile = chunk.finalizeChunk();

        String key = keyPrefix + String.format("%019d_%019d",startOffset,endOffset);
        logger.debug(String.format("Uploading chunk to S3 (%s).", key));
        s3Client.putObject(bucket, key, tmpChunkFile);

        maxCommittedOffset = endOffset;
        committedMessageCount += chunk.getTotalMessageCount();
        committedMessageSize += chunk.getTotalMessageSize();

        chunk.cleanUp();

        chunk = Chunk.createChunk(partition, maxCommittedOffset);
	}

    @Override
    public long getMaxCommittedOffset() {
        return maxCommittedOffset;
    }

    private long fetchLastCommittedOffset() {

        List<S3ObjectSummary> objectSummaries =
                s3Client.listObjects(
                        new ListObjectsRequest()
                                .withBucketName(bucket)
                                .withDelimiter("/")
                                .withPrefix(keyPrefix)
                ).getObjectSummaries();

        long maxOffset = 0;

        for (S3ObjectSummary objectSummary : objectSummaries) {
            String[] offsets = objectSummary.getKey().substring(keyPrefix.length()).split("_");
            long endOffset = Long.valueOf(offsets[1]);
            if (endOffset > maxOffset)
                maxOffset = endOffset;
        }
        return maxOffset;
    }

    @Override
    public long getUncommittedMessageSize() {
        return chunk.getTotalMessageSize();
    }

    @Override
    public long getLastCommitTimestamp() {
        return lastCommitTimestamp;
    }

    @Override
    public String toString() {
        return String.format("S3Sink(bucket=%s,prefix=%s,committedMessageCount=%d,committedMessageSize=%d,uncommittedMessageCount=%d,uncommittedMessageSize=%d,startOffset=%d,endOffset=%d)",bucket, keyPrefix, committedMessageCount, committedMessageSize, chunk.getTotalMessageCount(), chunk.getTotalMessageSize(), chunk.getStartOffset(), chunk.getEndOffset());
    }

    protected static class Chunk {

        public static Chunk createChunk(Partition partition, long startOffset) throws IOException {
            return new Chunk(partition, startOffset);
        }

        private Partition partition;

        private long startOffset;
        private long endOffset;

        private long totalMessageCount = 0;
        private long totalMessageSize = 0;

        private File tmpFile;
        private OutputStream tmpOutputStream;
        SequenceFile.Writer tmpWriter;

        private Text key = new Text();
        private BytesWritable value = new BytesWritable();

        protected Chunk(Partition partition, long startOffset) throws IOException {

            this.partition = partition;
            this.startOffset = this.endOffset  = startOffset;

            tmpFile = File.createTempFile("s3sink", null);
            tmpOutputStream = new FileOutputStream(tmpFile);
            org.apache.hadoop.conf.Configuration hadoopConf = new org.apache.hadoop.conf.Configuration(false);
            tmpWriter = SequenceFile.createWriter(hadoopConf, new FSDataOutputStream(tmpOutputStream, new FileSystem.Statistics("")), Text.class, BytesWritable.class, SequenceFile.CompressionType.BLOCK, new BZip2Codec());
            logger.debug("Created tmpFile: " + tmpFile);
        }

        public long getStartOffset() {
            return startOffset;
        }

        public long getEndOffset() {
            return endOffset;
        }

        public long getTotalMessageCount() {
            return totalMessageCount;
        }

        public long getTotalMessageSize() {
            return totalMessageSize;
        }

        public void appendMessageToChunk(MessageAndOffset messageAndOffset) throws IOException {

            key.set(String.format("%s.%d.%d.%d", partition.getTopic(), partition.getBrokerId(), partition.getPartitionId(), messageAndOffset.offset()));

            int messageSize = messageAndOffset.message().payload().remaining();
            totalMessageCount += 1;
            totalMessageSize += messageSize;
            endOffset = messageAndOffset.offset();

            byte[] buffer = new byte[messageSize];
            messageAndOffset.message().payload().get(buffer);
            value.set(buffer, 0, messageSize);
            tmpWriter.append(key, value);
        }

        public File finalizeChunk() throws IOException {
            tmpWriter.close();
            tmpOutputStream.close();
            return tmpFile;
        }

        public void cleanUp() {
            tmpFile.delete();
        }

        public boolean isEmpty() {
            return startOffset == endOffset;
        }
    }
}
