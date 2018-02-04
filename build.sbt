name := "DoubleTopic"

version := "0.1"

scalaVersion := "2.11.8"

libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "2.7.3"

libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.2.0" % "provided"

libraryDependencies += "org.apache.spark" % "spark-streaming_2.11" % "2.2.0" % "provided"

libraryDependencies += "org.apache.spark" % "spark-streaming-kafka-0-8_2.11" % "2.2.0"


        