#!/bin/sh

# paths
base_dir=$(dirname $0)
bundletmpdir=${base_dir}/bundle/tmp
bundleroot=${bundletmpdir}/kafka-archiver

jar_file=${base_dir}/target/kafka-s3-consumer-1.0.jar

# package jar
mvn clean package

# clean up
rm -rf ${bundletmpdir}

# prepare
mkdir ${bundletmpdir}
mkdir ${bundleroot}

for dir in bin config lib log; do
  mkdir ${bundleroot}/${dir}
done

# bundling archive

echo "moving jar into place"
cp ${jar_file} ${bundleroot}/lib/kafka-archiver.jar

echo "moving maintenance scripts into place"
cp -r ${base_dir}/bundle/bin ${bundleroot}/

echo "moving config files into place"
cp -r ${base_dir}/bundle/config ${bundleroot}/

echo "bundling archive"
tar -czf ${base_dir}/target/kafka-archiver.tgz -C ${bundletmpdir} kafka-archiver

echo "created bundle in ${base_dir}/target/kafka-archiver.tgz"
