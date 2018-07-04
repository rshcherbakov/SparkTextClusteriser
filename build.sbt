name := "sciProject"

version := "0.1"

scalaVersion := "2.11.6"

//resolvers += Resolver.bintrayIvyRepo("com.eed3si9n", "sbt-plugins")
val sparkVersion = "2.3.1"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion,
  //"org.apache.spark" %% "spark-streaming-twitter" % s"$sparkVersion-SNAPSHOT",
  "org.apache.spark" %% "spark-sql" % sparkVersion
)