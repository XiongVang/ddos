name := "ddos"

version := "0.1"

scalaVersion := "2.11.12"

val sparkVersion = "2.3.3"

resolvers ++= Seq(
  "bintray-spark-packages" at "https://dl.bintray.com/spark-packages/maven/",
  "Typesafe Simple Repository" at "http://repo.typesafe.com/typesafe/simple/maven-releases/",
  "MavenRepository" at "https://mvnrepository.com/"
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-streaming" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-streaming-kafka-0-10" % "2.3.0" % "provided"
)