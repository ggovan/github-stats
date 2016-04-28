package githubstats

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import net.liftweb.json
import net.liftweb.json.DefaultFormats

import akka.actor.{ ActorSystem, Props }
import akka.io.IO
import spray.can.Http
import akka.pattern.ask
import akka.util.Timeout
import scala.concurrent.duration._

import org.apache.spark._

object GithubStats {

  var gitHubStats: GithubStats = _

  def main(args: Array[String]): Unit = {
    val pathToEvents = args(0);
    val pathToCommits = args(1);

    println(pathToEvents);
    println(pathToCommits);

    val sc = new SparkContext()

    val ghs = new GithubStats(sc, pathToCommits, pathToEvents)

    //val filesString = ghs.fileCounts()

    println(s"There are ${ghs.events.count()} events with ${ghs.commits.count()} commits that modify ${ghs.files.count()} files.");
    //println(filesString)

    gitHubStats = ghs

    implicit val system = ActorSystem("on-spray-can")

    // create and start our service actor
    val service = system.actorOf(Props[StatsServiceActor], "demo-service")

    implicit val timeout = Timeout(200.seconds)
    // start a new HTTP server on port 8080 with our service actor as the handler
    IO(Http) ? Http.Bind(service, interface = "localhost", port = 9000)

  }
}

class GithubStats(sc: SparkContext, pathToCommits: String, pathToEvents: String) {

  var commitFiles =  new java.io.File(pathToCommits).listFiles
      .map(_.getCanonicalPath)
      .filter(_.endsWith(".json"))
      .toSet

  var commits = sc.wholeTextFiles(pathToCommits)
    .flatMap {
      case (filename, contents) =>
        implicit val formats = DefaultFormats + new PayloadSerializer
        json.parse(contents)
          .extract[List[CommitResponse]]
    }.cache()

  var events = sc.wholeTextFiles(pathToEvents)
    .flatMap {
      case (filename, contents) =>
        implicit val formats = DefaultFormats + new PayloadSerializer
        json.parse(contents)
          .extract[List[GitHubEvent[PushEventPayload]]]
    }.cache()

  // These files are not unique. The same file may be touched multiple times in different commits.
  var files = commits.flatMap { c => c.files.map(_.filename) }.cache()

  def fileCounts(): List[(String,Long)] = files
    .map(fn => if (fn.contains('.')) fn.substring(fn.lastIndexOf('.')) else "No suffix")
    .countByValue().toList
    .sortBy(-_._2).take(50)
    //.mkString("\n")

  def newFiles(): List[String] = {
    val processed = commitFiles
    val all = new java.io.File(pathToCommits).listFiles
      .map(_.getCanonicalPath)
      .filter(_.endsWith(".json"))
      .toSet
    commitFiles = all
    (all -- processed).toList
  }
    
  def makeCommits(files: List[String]): RDD[CommitResponse] = {
    files.map(sc.wholeTextFiles(_))
      .foldLeft[RDD[(String, String)]](sc.emptyRDD)(_ union _)
      .flatMap {
        case (filename, contents) =>
          implicit val formats = DefaultFormats + new PayloadSerializer
          println(filename)
          json.parse(contents)
            .extract[List[CommitResponse]]
      }.cache()
  }

  def makeFiles(commits: RDD[CommitResponse]): RDD[String] =
    commits.flatMap { c => c.files.map(_.filename) }.cache()
    
  def update():Boolean = {
    val newFiles = this.newFiles()
    val newCommits = makeCommits(newFiles)
    val newFileNames = makeFiles(newCommits)
    
    commits = commits.union(newCommits).cache
    files = files.union(newFileNames).cache
    
    true
  }

}
