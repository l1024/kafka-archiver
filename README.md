kafka-archiver
==============

Archive Kafka messages into sequence files in S3.
All messages of on topic will be stored in sequence files under one directory. Each sequence file will contain a chunk of messages.
Key will be 'topic.brokerId.partitionId.offset' stored as Text. Value will be the binary message content stored as BytesWritable.

Configurable options
--------------------
- topics which should be archived
- total message size per chunk
- s3 bucket/prefix
- maximum interval between chunk uploads

With 'mybucket' and 'myarchive' configured as s3 bucket/prefix you will end up with the following files in s3:

mybucket/myarchive/<topic>/<broker_id>_<partition_id>_<start_offset>_<end_offset>

Build
-----
```
$ cd <project root>
edit config files in ./bundle/config as appropriate
$ ./bundle.sh
```

creates ./target/kafka-archiver.tgz

Run
---
```
$ tar xzf kafka-archiver.tgz
$ cd kafka-archiver
$ ./bin/kafka-archiver-start.sh
```

Stop/Restart
------------

It is safe to just kill and restart the daemon. It will resume its operation where it left off.