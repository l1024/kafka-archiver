package org.l1024.kafka.archiver.s3;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.S3Object;
import junit.framework.TestCase;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.Message;
import kafka.message.MessageAndOffset;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;
import org.junit.Test;
import org.l1024.kafka.archiver.Partition;
import org.l1024.kafka.archiver.Sink;
import org.l1024.kafka.archiver.SinkFactory;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.UUID;

/**
 * Created with IntelliJ IDEA.
 * User: lorenz
 * Date: 8/10/12
 * Time: 9:29 AM
 * To change this template use File | Settings | File Templates.
 */
public class S3SinkTest extends TestCase {

    private static final Logger logger = Logger.getLogger(S3SinkTest.class);

    private static final byte[] message1 = {'m', 'e', 's', 's', 'a', 'g', 'e', '_', '1'};
    private static final byte[] message2 = {'m', 'e', 's', 's', 'a', 'g', 'e', '_', '2'};

    @Test
    public void testChunk() throws IOException {

        Partition partition = new Partition("topic.test", 3, 2);
        new ByteBufferMessageSet(Arrays.asList(new Message(message1), new Message(message2)));
        S3Sink.Chunk chunk = new S3Sink.Chunk(partition, 42L);

        chunk.appendMessageToChunk(new MessageAndOffset(new Message(message1), 72));

        assertEquals(42L, chunk.getStartOffset());
        assertEquals(72L, chunk.getEndOffset());
        assertEquals(1L, chunk.getTotalMessageCount());
        assertEquals(9L, chunk.getTotalMessageSize());

        chunk.appendMessageToChunk(new MessageAndOffset(new Message(message2), 102));

        assertEquals(42L, chunk.getStartOffset());
        assertEquals(102L, chunk.getEndOffset());
        assertEquals(2L, chunk.getTotalMessageCount());
        assertEquals(18L, chunk.getTotalMessageSize());

        File file = chunk.finalizeChunk();
        Path path = new Path(file.toURI());

        org.apache.hadoop.conf.Configuration hadoopConf = new org.apache.hadoop.conf.Configuration(false);
        hadoopConf.set("fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem");
        FileSystem fs = FileSystem.get(file.toURI(), hadoopConf);
        SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, hadoopConf);

        assertEquals(Text.class, reader.getKeyClass());
        assertEquals(BytesWritable.class, reader.getValueClass());

        Text key = new Text();
        BytesWritable value = new BytesWritable();
        assertTrue(reader.next(key, value));
        assertEquals("topic.test:3:2:72", key.toString());
        assertEquals("message_1", new String(value.getBytes(), 0, value.getLength(), "UTF-8"));
        assertTrue(reader.next(key, value));
        assertEquals("topic.test:3:2:102", key.toString());
        assertEquals("message_2", new String(value.getBytes(), 0, value.getLength(), "UTF-8"));
        assertFalse(reader.next(key, value));
        chunk.cleanUp();
    }

    @Test
    public void testSink() throws IOException {

        String s3AccessKey=System.getProperty("s3.accessKey");
        String s3SecretKey= System.getProperty("s3.secretKey");
        String bucket=System.getProperty("s3.bucket");
        String prefix=System.getProperty("s3.prefix");

        if (s3AccessKey == null || s3SecretKey == null || bucket == null || prefix == null) {
            logger.warn("Skipping test testSink. Please check test configuration in build.sbt.");
            assertTrue(true);
            return;
        }

        Partition partition = new Partition("unit_test_topic_" + UUID.randomUUID().toString().replace("-", ""), 3, 2);

        long startOffset = 0L;
        long endOffset = 20L;

        SinkFactory sinkFactory = new SinkFactory(s3AccessKey, s3SecretKey, bucket, prefix);
        Sink sink = sinkFactory.createSink(partition);
        assertEquals(0L, sink.getMaxCommittedOffset());
        sink.append(new MessageAndOffset(new Message(message1), endOffset));
        sink.commitChunk();

        AmazonS3Client s3Client = new AmazonS3Client(new BasicAWSCredentials(s3AccessKey, s3SecretKey));
        String key = prefix + "/" + partition.getTopic() + "/" + partition.getBrokerId() + "_" + partition.getPartitionId() + "_0000000000000000000_0000000000000000020";
        S3Object obj = s3Client.getObject(bucket, key);

        assertNotNull(obj);
        assertEquals(bucket, obj.getBucketName());
        assertEquals(key, obj.getKey());

        sink = sinkFactory.createSink(partition);
        assertEquals(20L, sink.getMaxCommittedOffset());
        s3Client.deleteObject(bucket, key);
    }
}
