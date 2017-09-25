name := "StreamEvents"

version := "0.1"

scalaVersion := "2.11.11"
val sparkVersion = "2.1.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "com.github.scopt" %% "scopt" % "3.7.0",
  "org.mockito" % "mockito-core" % "1.8.5" % "test"
)
