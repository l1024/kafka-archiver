package org.l1024.kafka.archiver;

import kafka.message.MessageAndOffset;

import java.io.IOException;

public interface Sink {

    public void append(MessageAndOffset messageAndOffset)
            throws IOException;

    public void commitChunk() throws IOException;

    public long getMaxCommittedOffset();

    public long getUncommittedMessageSize();

    public long getLastCommitTimestamp();

}