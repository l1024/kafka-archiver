kafka-archiver
=================

Archive Kafka messages in S3.

Just start a kafka-archiver daemon on each kafka broker.

Build

```
$ cd <project root>
edit config files in ./bundle/config as appropriate
$ ./bundle.sh
```

creates ./target/kafka-archiver.tgz

Run

```
$ tar xzf kafka-archiver.tgz
$ cd kafka-archiver
$ ./bin/kafka-archiver-start.sh
```
