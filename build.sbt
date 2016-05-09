name := "github-stats"

version := "0.0.1"

// 2.10 is more widely supported by Spark than 2.11
scalaVersion := "2.10.6"

scalacOptions := Seq("-unchecked", "-deprecation", "-encoding", "utf8")

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.5.1" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "1.5.1" % "provided"

libraryDependencies += "net.liftweb" %% "lift-json" % "2.6.3"
libraryDependencies += "redis.clients" % "jedis" % "2.8.0"
libraryDependencies += "com.github.etaty" %% "rediscala" % "1.6.0"

libraryDependencies ++= {
  val akkaV = "2.3.9"
  val sprayV = "1.3.3"
  Seq(
    "io.spray"            %%  "spray-can"     % sprayV,
    "io.spray"            %%  "spray-routing" % sprayV,
    "io.spray"            %%  "spray-json"    % "1.3.2",
    "io.spray"            %%  "spray-testkit" % sprayV  % "test",
    "com.typesafe.akka"   %%  "akka-actor"    % akkaV,
    "com.typesafe.akka"   %%  "akka-testkit"  % akkaV   % "test",
    "org.specs2"          %%  "specs2-core"   % "2.3.7" % "test"
  )
}

Revolver.settings

// Testing 

libraryDependencies += "com.holdenkarau" %% "spark-testing-base" % "1.6.1_0.3.3" % Test

libraryDependencies += "org.scalacheck" %% "scalacheck" % "1.12.5" % Test

libraryDependencies +=  "org.scalaj" %% "scalaj-http" % "2.3.0"

javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:MaxPermSize=2048M", "-XX:+CMSClassUnloadingEnabled")

parallelExecution in Test := false