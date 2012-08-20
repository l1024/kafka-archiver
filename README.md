# kafka-archiver

## Description

Archive Kafka messages into sequence files in S3.
All messages of a topic will be stored in sequence files under one directory. Each sequence file will contain a chunk of messages.
Key will be '&lt;topic>:&lt;brokerId>:&lt;partitionId>:&lt;offset>' stored as Text. Value will be the binary message content stored as BytesWritable.

### Configurable options
- topics which should be archived
- total message size per chunk
- s3 bucket/prefix
- maximum interval between chunk uploads

With 'mybucket' and 'myarchive' configured as s3 bucket/prefix you will end up with the following files in s3:

s3://mybucket/myarchive/&lt;topic>/&lt;broker\_id>\_&lt;partition\_id>\_&lt;start\_offset>\_&lt;end\_offset>

## Operations

Just start one kafka-archiver daemon on each kafka broker.

### Build
```
$ cd <project root>
edit config files in ./bundle/config as appropriate
$ ./bundle.sh
```

creates ./target/kafka-archiver.tgz

### Start

A simple start script is provided to start the kafka-archiver daemon. This script will prevent the user from starting multiple instances of the archiver on one machine.

```
$ tar xzf kafka-archiver.tgz
$ cd kafka-archiver
$ ./bin/kafka-archiver-start.sh
```

### Stop/Restart

It is safe to just kill and restart the daemon. It will resume its operation where it left off.

A stop script is provided for convenience. However, this script will ask for confirmation before killing the process.

```
$ ./bin/kafka-archiver-stop.sh
Running kafka-archiver found:
86051 s001  S      0:08.95 java -cp ./bin/../config:./bin/../lib/kafka-archiver.jar org.l1024.kafka.archiver.Archiver ...
Kill archiver with pid 86051 ? [y/N]
y
$ ./bin/kafka-archiver-start.sh
```