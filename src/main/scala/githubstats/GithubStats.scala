package githubstats

import scala.annotation.tailrec
import scala.concurrent.duration.DurationInt

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.LocatedFileStatus
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.RemoteIterator
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import akka.actor.ActorSystem
import akka.actor.Props
import akka.io.IO
import akka.pattern.ask
import akka.util.Timeout
import net.liftweb.json
import net.liftweb.json.DefaultFormats
import spray.can.Http

object GithubStats {

  var gitHubStats: GithubStats = _

  def main(args: Array[String]): Unit = {
    val pathToEvents = args(0);
    val pathToCommits = args(1);

    println(pathToEvents);
    println(pathToCommits);

    val sc = new SparkContext()

    val ghs = new GithubStats(sc, pathToCommits, pathToEvents)

    gitHubStats = ghs

    implicit val system = ActorSystem("on-spray-can")

    // create and start our service actor
    val service = system.actorOf(Props[StatsServiceActor], "demo-service")

    implicit val timeout = Timeout(200.seconds)
    // start a new HTTP server on port 8080 with our service actor as the handler
    IO(Http) ? Http.Bind(service, interface = "0.0.0.0", port = 9021)

  }
}

class GithubStats(sc: SparkContext, pathToCommits: String, pathToEvents: String) {

  private var commitFilesWithPaths = sc.wholeTextFiles(pathToCommits)
  
  var commitFiles = commitFilesWithPaths
    .map(_._1).collect().toList

  var commits = commitFilesWithPaths
    .flatMap {
      case (filename, contents) =>
        implicit val formats = DefaultFormats + new PayloadSerializer
        json.parse(contents)
          .extract[List[CommitResponse]]
    }.cache()

  // These files are not unique. The same file may be touched multiple times in different commits.
  var files = commits.flatMap { c => c.files.map(_.filename) }.cache()

  def fileCounts(limit: Int): List[(String,Long)] = files
    .map(fn => if (fn.contains('.')) fn.substring(fn.lastIndexOf('.')) else "No suffix")
    .countByValue().toList
    .sortBy(-_._2).take(limit)

  def newFiles(): List[String] = {
    val processed = commitFiles
    
    val allRI = FileSystem.get(sc.hadoopConfiguration).listFiles(new Path(pathToCommits), true)
    
    @tailrec
    def riToList(ri: RemoteIterator[LocatedFileStatus], acc: List[LocatedFileStatus]): List[LocatedFileStatus] = 
      if(ri.hasNext) riToList(ri, ri.next :: acc) else acc
    
    val all = riToList(allRI, Nil).map(_.getPath.toString)
    
    commitFiles = all
    println(all)
    all.toSet.--(processed).toList
  }
    
  def makeCommits(files: List[String]): RDD[CommitResponse] = {
    println(files)
    files.map(sc.wholeTextFiles(_))
      .fold(sc.emptyRDD)(_ union _)
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
