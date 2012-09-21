package org.l1024.kafka.archiver.config;

import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

public class PropertyConfiguration extends Configuration {

  private final Properties props;

  private static final String PROP_S3_ACCESS_KEY = "s3.accesskey";
  private static final String PROP_S3_SECRET_KEY = "s3.secretkey";
  private static final String PROP_S3_BUCKET = "s3.bucket";
  private static final String PROP_S3_PREFIX = "s3.prefix";
  private static final String PROP_S3_MIN_TOTAL_MESSAGE_SIZE_PER_CHUNK = "s3.mintotalmessagesizeperchunk";
  private static final String PROP_S3_MAX_COMMIT_INTERVAL = "s3.maxcommitinterval";
  private static final String PROP_KAFKA_MAX_MESSAGE_SIZE = "kafka.maxmessagesize";
  private static final String PROP_KAFKA_TOPICS = "kafka.topics";
  private static final String PROP_IGNORE_GAPS_TOPICS = "kafka.ignoregaps";

  public PropertyConfiguration(Properties props) {
    this.props = props;
  }

  @Override
  public String getS3AccessKey() {
    String s3AccessKey = props.getProperty(PROP_S3_ACCESS_KEY);
    if (s3AccessKey == null || s3AccessKey.isEmpty()) {
      throw new RuntimeException("Invalid property " + PROP_S3_ACCESS_KEY);
    }
    return s3AccessKey;
  }

  @Override
  public String getS3SecretKey() {
    String s3SecretKey = props.getProperty(PROP_S3_SECRET_KEY);
    if (s3SecretKey == null || s3SecretKey.isEmpty()) {
      throw new RuntimeException("Invalid property " + PROP_S3_SECRET_KEY);
    }
    return s3SecretKey;
  }

  @Override
  public String getS3Bucket() {
    String s3Bucket = props.getProperty(PROP_S3_BUCKET);
    if (s3Bucket == null || s3Bucket.isEmpty()) {
      throw new RuntimeException("Invalid property " + PROP_S3_BUCKET);
    }
    return s3Bucket;
  }

  @Override
  public String getS3Prefix() {
    String s3Prefix = props.getProperty(PROP_S3_PREFIX);
    if (s3Prefix == null || s3Prefix.isEmpty()) {
      throw new RuntimeException("Invalid property " + PROP_S3_PREFIX);
    }
    return s3Prefix.replaceAll("/$", "");
  }

  @Override
  public Set<String> getTopics() {
    Set<String> result = new HashSet<String>();
    String kafkaTopics = props.getProperty(PROP_KAFKA_TOPICS);
    if (kafkaTopics == null || kafkaTopics.isEmpty()) {
      throw new RuntimeException("Invalid property " + PROP_KAFKA_TOPICS);
    }
    for (String topic: kafkaTopics.split(",")) {
      result.add(topic);
    }
    return result;
  }

  @Override
  public Set<String> getIgnoreGapsTopics() {
    Set<String> result = new HashSet<String>();
    String kafkaIgnoreGapsTopics = props.getProperty(PROP_IGNORE_GAPS_TOPICS);
    if (kafkaIgnoreGapsTopics != null) {
        for (String topic: kafkaIgnoreGapsTopics.split(",")) {
            result.add(topic);
        }
    }
    return result;
  }

  @Override
  public long getMinTotalMessageSizePerChunk() {
    String minTotalMessageSizePerChunk = props.getProperty(PROP_S3_MIN_TOTAL_MESSAGE_SIZE_PER_CHUNK);
    if (minTotalMessageSizePerChunk == null || minTotalMessageSizePerChunk.isEmpty()) {
      return 268435456;
    }
    return Long.valueOf(minTotalMessageSizePerChunk);

  }

  @Override
  public int getKafkaMaxMessageSize() {
    String maxMessageSize = props.getProperty(PROP_KAFKA_MAX_MESSAGE_SIZE);
    if (maxMessageSize == null || maxMessageSize.isEmpty()) {
      return 65536;
    }
    return Integer.valueOf(maxMessageSize);

  }

  @Override
  public int getMaxCommitInterval() {
      String maxCommitInterval = props.getProperty(PROP_S3_MAX_COMMIT_INTERVAL);
      if (maxCommitInterval == null || maxCommitInterval.isEmpty()) {
          return 86400000;
      }
      return Integer.valueOf(maxCommitInterval);
  }
}
