package kafka.s3.consumer;

import java.io.IOException;

import kafka.message.MessageAndOffset;

interface Sink {

	public abstract long append(MessageAndOffset messageAndOffset)
			throws IOException;

	public abstract long getMaxCommittedOffset();

}