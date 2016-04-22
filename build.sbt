name := "github-stats"

version := "0.0.1"

// 2.10 is more widely supported by Spark than 2.11
scalaVersion := "2.10.6"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.5.1" % "provided"

// not streaming, but likely to add soon
//libraryDependencies += "org.apache.spark" %% "spark-streaming" % "1.5.1" % "provided"

libraryDependencies += "net.liftweb" %% "lift-json" % "2.6.3"
