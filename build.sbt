name := "spark-in-practice-scala"

version := "1.0"

scalaVersion := "2.11.12"

libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.3.1"

libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "2.3.1"

libraryDependencies += "org.apache.spark" % "spark-mllib_2.11" % "2.3.1"

libraryDependencies += "org.apache.spark" % "spark-streaming_2.11" % "2.3.1"

libraryDependencies += "com.fasterxml.jackson.core" % "jackson-databind" % "2.4.4"

libraryDependencies += "com.google.code.gson" % "gson" % "2.3.1"

libraryDependencies += "org.apache.spark" % "spark-streaming-twitter_2.11" % "1.6.2"

libraryDependencies += "org.twitter4j" % "twitter4j-core" % "4.0.6"

libraryDependencies += "org.scalatest" % "scalatest_2.11" % "2.2.2" % "test"

resolvers += "Akka Repository" at "http://repo.akka.io/releases/"
