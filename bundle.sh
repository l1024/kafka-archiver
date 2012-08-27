#!/bin/sh

# paths
base_dir=$(dirname $0)
bundletmpdir=`mktemp -d -t "kafka-archiver-bundle"`
bundleroot=${bundletmpdir}/kafka-archiver

jar_file=${base_dir}/target/kafka-archiver-assembly-0.1-SNAPSHOT.jar

# package jar
sbt/sbt assembly

echo "Creating bundle in tmpdir: ${bundletmpdir}"

# prepare
for dir in '' 'lib' 'log'; do
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

# cleanup
rm -rf ${bundletmpdir}

