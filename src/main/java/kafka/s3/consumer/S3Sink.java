package kafka.s3.consumer;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.List;

import kafka.message.MessageAndOffset;

import org.apache.log4j.Logger;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.S3ObjectSummary;

class S3Sink implements Sink {

    private static final Logger logger = Logger.getLogger(S3Sink.class);

    private Configuration conf;

    private String topic;
    private int partition;

    private String bucket;
    private AmazonS3Client awsClient;

    long startOffset;
    long endOffset;
    int bytesWritten;

    File tmpFile;
    OutputStream tmpOutputStream;
    WritableByteChannel tmpChannel;

    public S3Sink(String topic, int partition, Configuration conf) throws FileNotFoundException, IOException {

      this.conf = conf;

      this.topic = topic;
      this.partition = partition;

      bucket = conf.getS3Bucket();
      awsClient = new AmazonS3Client(new BasicAWSCredentials(conf.getS3AccessKey(), conf.getS3SecretKey()));

      startOffset = endOffset = fetchLastCommittedOffset();
      bytesWritten = 0;

      tmpFile = File.createTempFile("s3sink", null);
      logger.debug("Created tmpFile: " + tmpFile);
      tmpOutputStream = new FileOutputStream(tmpFile);
      tmpChannel = Channels.newChannel(tmpOutputStream);
    }

    @Override
    public void append(MessageAndOffset messageAndOffset) throws IOException {

      int messageSize = messageAndOffset.message().payload().remaining();
      logger.debug("Appending message with size: " + messageSize);

      if (bytesWritten + messageSize + 1 > conf.getS3MaxObjectSize()) {
        logger.debug("Uploading chunk to S3. Size is: " + bytesWritten);
        String key = getKeyPrefix() + startOffset + "_" + endOffset;
        awsClient.putObject(bucket, key, tmpFile);
        tmpChannel.close();
        tmpOutputStream.close();
        tmpFile.delete();
        tmpFile = File.createTempFile("s3sink", null);
        logger.debug("Created tmpFile: " + tmpFile);
        tmpOutputStream = new FileOutputStream(tmpFile);
        tmpChannel = Channels.newChannel(tmpOutputStream);
        startOffset = endOffset;
        bytesWritten = 0;
      }

      tmpChannel.write(messageAndOffset.message().payload());
      tmpOutputStream.write('\n');
      bytesWritten += messageSize + 1;

      endOffset = messageAndOffset.offset();
    }

    @Override
    public long getMaxCommittedOffset() {
      return startOffset;
    }

    private long fetchLastCommittedOffset() {
      logger.debug("Getting max offset for " + topic + ":" + partition);
      String prefix = getKeyPrefix();
      logger.debug("Listing keys for bucket/prefix " + bucket + "/" + prefix);
      List<S3ObjectSummary> objectSummaries = awsClient.listObjects(new ListObjectsRequest().withBucketName(bucket).withDelimiter("/").withPrefix(prefix)).getObjectSummaries();
      logger.debug("Received result " + objectSummaries);

      long maxOffset = 0;

      for (S3ObjectSummary objectSummary : objectSummaries) {
        logger.debug(objectSummary.getKey());
        String[] offsets = objectSummary.getKey().substring(prefix.length()).split("_");
        long endOffset = Long.valueOf(offsets[1]);
        if (endOffset > maxOffset)
          maxOffset = endOffset;
      }
      return maxOffset;
    }

    private String getKeyPrefix() {
      return conf.getS3Prefix() + "/" + topic + "/" + conf.getKafkaBrokerId() + "_" + partition + "_";
    }
}