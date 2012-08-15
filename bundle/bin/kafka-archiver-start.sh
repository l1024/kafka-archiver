#!/bin/sh

########
#
# Environment Variables (can be set in config/kafka-archiver-env.sh)
#
# JAVA_HOME
#
# KAFKA_HOME
#
########

base_dir=$(dirname $0)/..

if [ ! -z "`${base_dir}/bin/kafka-archiver-test.sh`" ]; then
  echo "Kafka-archiver already running. Please stop it first.";
  exit 2;
fi

if [ -f "${base_dir}/config/kafka-archiver-env.sh" ]; then
  . "${base_dir}/config/kafka-archiver-env.sh"
fi

if [ -z "$JAVA_HOME" ]; then
  JAVA="java"
else
  JAVA="$JAVA_HOME/bin/java"
fi

if [ -z "$KAFKA_HOME" ]; then
  echo "KAFKA_HOME must be set."
  exit 1;
fi

kafka_config=${KAFKA_HOME}/config/server.properties
archiver_config=${base_dir}/config/archiver.properties
log_file=${base_dir}/log/kafka-archiver.log

CLASSPATH="${base_dir}/config"

for PATH in `ls ${base_dir}/lib/*.jar`; do
  CLASSPATH=$CLASSPATH:$PATH;
done

NOHUP=/usr/bin/nohup

echo "${NOHUP} ${JAVA} -cp ${CLASSPATH}  org.l1024.kafka.archiver.Archiver ${kafka_config} ${archiver_config} 2>&1 > ${log_file} &"
${NOHUP} ${JAVA} -cp ${CLASSPATH}  org.l1024.kafka.archiver.Archiver ${kafka_config} ${archiver_config} 2>&1 > ${log_file} &

