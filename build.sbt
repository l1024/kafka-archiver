import AssemblyKeys._ // put this at the top of the file

assemblySettings

name := "kafka-archiver"

scalaVersion := "2.8.2"

libraryDependencies += "log4j" % "log4j" %"1.2.15" exclude("com.sun.jmx", "jmxri") exclude("com.sun.jdmk", "jmxtools") exclude("javax.jms", "jms")

libraryDependencies += "com.github.sgroschupf" % "zkclient" % "0.1"

libraryDependencies += "com.amazonaws" % "aws-java-sdk" % "1.3.14"

libraryDependencies += "org.apache.hadoop" % "hadoop-core" % "0.20.205.0"

libraryDependencies += "com.novocode" % "junit-interface" % "0.9-RC3" % "test"

mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) =>
  {
    case m if m.startsWith("javax/servlet/") => MergeStrategy.first
    case m if m.startsWith("org/apache/jasper/") => MergeStrategy.first
    case m if m.startsWith("org/apache/commons/") => MergeStrategy.first
    case x => old(x)
  }
}

