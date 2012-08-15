#!/bin/sh

base_dir=$(dirname $0)/..

kafka_ps="`${base_dir}/bin/kafka-archiver-test.sh`"

if [ -z "$kafka_ps" ]; then
  echo "No kafka-archiver running.";
  exit 0;
else
  echo "Running kafka-archiver found:"
  echo "$kafka_ps"
  pid="`echo \"$kafka_ps\" | awk '{print $1}'`"
  echo "Kill archiver with pid $pid ? [y/N]";
  read answer
  if [ "$answer" = "y" ]; then
    kill $pid
  fi
fi

