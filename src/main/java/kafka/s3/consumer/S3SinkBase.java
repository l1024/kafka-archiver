package kafka.s3.consumer;

import java.io.File;
import java.util.List;

import org.apache.log4j.Logger;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.S3ObjectSummary;

public class S3SinkBase {

	static final Logger logger = Logger.getLogger(S3SinkBase.class);

	private String bucket;
	private AmazonS3Client awsClient;

	private String keyPrefix;

	private long maxCommittedOffset;

	public S3SinkBase(String topic, int partition, Configuration conf) {
		super();
		keyPrefix = conf.getS3Prefix() + "/" + topic + "/" + conf.getKafkaBrokerId() + "_" + partition + "_";
		bucket = conf.getS3Bucket();
		awsClient = new AmazonS3Client(new BasicAWSCredentials(conf.getS3AccessKey(), conf.getS3SecretKey()));

		maxCommittedOffset = fetchLastCommittedOffset();
	}

	private String getKeyPrefix() {
		return keyPrefix;
	}

	public long getMaxCommittedOffset() {
		return maxCommittedOffset;
	}

	private long fetchLastCommittedOffset() {
		String prefix = getKeyPrefix();
		List<S3ObjectSummary> objectSummaries = awsClient.listObjects(new ListObjectsRequest().withBucketName(bucket).withDelimiter("/").withPrefix(prefix)).getObjectSummaries();
	
		long maxOffset = 0;

		for (S3ObjectSummary objectSummary : objectSummaries) {
			String[] offsets = objectSummary.getKey().substring(prefix.length()).split("_");
			long endOffset = Long.valueOf(offsets[1]);
			if (endOffset > maxOffset)
				maxOffset = endOffset;
		}
		return maxOffset;
	}

	protected void commitChunk(File chunk, long startOffset, long endOffset) {
		if (maxCommittedOffset != startOffset) {
			throw new RuntimeException("Gap between chunks detected.");
		}

		logger.debug("Uploading chunk to S3.");
		String key = getKeyPrefix() + startOffset + "_" + endOffset;
		awsClient.putObject(bucket, key, chunk);
		maxCommittedOffset = endOffset;
	}
}
