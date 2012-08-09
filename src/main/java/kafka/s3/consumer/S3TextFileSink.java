package kafka.s3.consumer;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;

import kafka.message.MessageAndOffset;

import org.apache.log4j.Logger;

class S3TextFileSink extends S3SinkBase implements Sink {

    static final Logger logger = Logger.getLogger(S3TextFileSink.class);

    private int s3MaxObjectSize;
    private long startOffset;
    private long endOffset;
    private int bytesWritten;

    File tmpFile;
    OutputStream tmpOutputStream;
    WritableByteChannel tmpChannel;

    public S3TextFileSink(String topic, int partition, Configuration conf) throws FileNotFoundException, IOException {
      super(topic,partition,conf);

      s3MaxObjectSize = conf.getS3MaxObjectSize();
      startOffset = endOffset = getMaxCommittedOffset();
      bytesWritten = 0;

      tmpFile = File.createTempFile("s3sink", null);
      tmpOutputStream = new FileOutputStream(tmpFile);
      tmpChannel = Channels.newChannel(tmpOutputStream);
      logger.debug("Created tmpFile: " + tmpFile);
    }

    @Override
    public long append(MessageAndOffset messageAndOffset) throws IOException {

      int messageSize = messageAndOffset.message().payload().remaining();
      logger.debug("Appending message with size: " + messageSize);

      if (bytesWritten + messageSize + 1 > s3MaxObjectSize) {
        tmpChannel.close();
        tmpOutputStream.close();
        commitChunk(tmpFile, startOffset, endOffset);
        tmpFile.delete();
        tmpFile = File.createTempFile("s3sink", null);
        tmpOutputStream = new FileOutputStream(tmpFile);
        tmpChannel = Channels.newChannel(tmpOutputStream);
        logger.debug("Created tmpFile: " + tmpFile);
        startOffset = endOffset;
        bytesWritten = 0;
      }

      tmpChannel.write(messageAndOffset.message().payload());
      tmpOutputStream.write('\n');
      bytesWritten += messageSize + 1;

      endOffset = messageAndOffset.offset();

      return messageSize;
    }
}