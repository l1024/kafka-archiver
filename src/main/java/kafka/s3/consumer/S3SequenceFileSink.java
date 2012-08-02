package kafka.s3.consumer;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import kafka.message.MessageAndOffset;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

class S3SequenceFileSink extends S3SinkBase implements Sink {

	static final Logger logger = Logger.getLogger(S3SequenceFileSink.class);

	private int s3MaxObjectSize;
	private long startOffset;
	private long endOffset;
	private int bytesWritten;
	private LongWritable key = new LongWritable();
	private BytesWritable value = new BytesWritable();

	File tmpFile;
	OutputStream tmpOutputStream;
	SequenceFile.Writer writer;

	public S3SequenceFileSink(String topic, int partition, Configuration conf) throws IOException {
		super(topic, partition, conf);

		s3MaxObjectSize = conf.getS3MaxObjectSize();
		startOffset = endOffset = getMaxCommittedOffset();
		bytesWritten = 0;

		tmpFile = File.createTempFile("s3sink", null);
		tmpOutputStream = new FileOutputStream(tmpFile);
		org.apache.hadoop.conf.Configuration hadoopConf = new org.apache.hadoop.conf.Configuration(false);
		writer = SequenceFile.createWriter(hadoopConf, new FSDataOutputStream(tmpOutputStream, new FileSystem.Statistics("")), LongWritable.class, BytesWritable.class, SequenceFile.CompressionType.NONE, null);
		logger.debug("Created tmpFile: " + tmpFile);
	}

	@Override
	public void append(MessageAndOffset messageAndOffset) throws IOException {
		int messageSize = messageAndOffset.message().payload().remaining();
		logger.debug("Appending message with size: " + messageSize);

		if (bytesWritten + messageSize > s3MaxObjectSize) {
			writer.close();
			tmpOutputStream.close();
			commitChunk(tmpFile, startOffset, endOffset);
			tmpFile.delete();
			tmpFile = File.createTempFile("s3sink", null);
			tmpOutputStream = new FileOutputStream(tmpFile);
			org.apache.hadoop.conf.Configuration hadoopConf = new org.apache.hadoop.conf.Configuration(false);
			writer = SequenceFile.createWriter(hadoopConf, new FSDataOutputStream(tmpOutputStream, new FileSystem.Statistics("")), LongWritable.class, BytesWritable.class, SequenceFile.CompressionType.NONE, null);
			logger.debug("Created tmpFile: " + tmpFile);
			startOffset = endOffset;
			bytesWritten = 0;
		}

		key.set(messageAndOffset.offset());
		byte[] buffer = new byte[messageSize];
		messageAndOffset.message().payload().get(buffer);
		value.set(buffer,0,messageSize);
		writer.append(key, value);
		bytesWritten += messageSize;

		endOffset = messageAndOffset.offset();
	}
}
