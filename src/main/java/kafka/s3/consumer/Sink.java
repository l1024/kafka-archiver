package kafka.s3.consumer;

import java.io.IOException;

import kafka.message.MessageAndOffset;

interface Sink {

	public abstract void append(MessageAndOffset messageAndOffset)
			throws IOException;

	public abstract long getMaxCommittedOffset();

}