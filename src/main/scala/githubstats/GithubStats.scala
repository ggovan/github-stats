package githubstats

import scala.annotation.tailrec

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.LocatedFileStatus
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.RemoteIterator
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import net.liftweb.json
import net.liftweb.json.DefaultFormats

object GithubStats {

  def main(args: Array[String]): Unit = {
    val pathToEvents = args(0);
    val pathToCommits = args(1);

    println(pathToEvents);
    println(pathToCommits);

    val sc = new SparkContext()

    val ghs = GithubStats(sc, pathToCommits)

    StatsService.start(ghs)
  }
  
  def apply(sc: SparkContext, pathToCommits: String): GithubStats = {
    val commitFilesWithPaths = sc.wholeTextFiles(pathToCommits)

    val commitFiles = commitFilesWithPaths
      .map(_._1).collect().toList

    val commits = commitFilesWithPaths
      .flatMap {
        case (filename, contents) =>
          implicit val formats = DefaultFormats + new GitHubEventSerializer
          json.parse(contents)
            .extract[List[CommitResponse]]
      }

    val files = commits.flatMap { c => c.files.map(_.filename) }.cache()
    
    GithubStats(sc, pathToCommits, commitFiles, commits, files)
  }
}

case class GithubStats(sc: SparkContext, pathToCommits: String, commitFiles: List[String], commits: RDD[CommitResponse], files: RDD[String]) {

  def fileCounts(limit: Int): List[(String,Long)] = files
    .map(fn => if (fn.contains('.')) fn.substring(fn.lastIndexOf('.')) else "None")
    .countByValue().toList
    .sortBy(-_._2).take(limit)
    
  def commitFileCounts(): Array[(String,Int)] = commits
    .map(c => c.files)
    .filter(files => !files.isEmpty)
    .map(files => {
        val extensionsMap = files.groupBy(fn => if (fn.filename.contains('.')) fn.filename.substring(fn.filename.lastIndexOf('.')) else "None")
          .mapValues(l => l.length)
          
        val mostFrequent = extensionsMap.keysIterator.reduceLeft((x,y) => if (extensionsMap(x) > extensionsMap(y)) x else y)
        val totalSize = extensionsMap.valuesIterator.foldLeft(0)(_ + _)
        (mostFrequent, totalSize)
     })
    .collect();
    
  def getMostFrequent(fileNames: List[Files]) : (String,Long) = {
    val extensionsMap = fileNames.groupBy(fn => if (fn.filename.contains('.')) fn.filename.substring(fn.filename.lastIndexOf('.')) else "None")
    .mapValues(l => l.length)
    
    val mostFrequent = extensionsMap.keysIterator.reduceLeft((x,y) => if (extensionsMap(x) > extensionsMap(y)) x else y)
    val totalSize = extensionsMap.valuesIterator.foldLeft(0)(_ + _)
    (mostFrequent, totalSize)
  }
    
    
  def newFiles(): List[String] = {
    val processed = commitFiles
    
    val allRI = FileSystem.get(sc.hadoopConfiguration).listFiles(new Path(pathToCommits), true)
    
    @tailrec
    def riToList(ri: RemoteIterator[LocatedFileStatus], acc: List[LocatedFileStatus]): List[LocatedFileStatus] = 
      if(ri.hasNext)
        riToList(ri, ri.next :: acc)
      else acc
    
    val allFiles = riToList(allRI, Nil).map(_.getPath.toString)
    allFiles.toSet.--(processed).toList
  }
    
  def makeCommits(files: List[String]): RDD[CommitResponse] = {
    println(files)
    files.map(sc.wholeTextFiles(_))
      .fold(sc.emptyRDD)(_ union _)
      .flatMap {
        case (filename, contents) =>
          implicit val formats = DefaultFormats + new GitHubEventSerializer
          println(filename)
          json.parse(contents)
            .extract[List[CommitResponse]]
      }
  }

  def update(): GithubStats = {
    val newFiles = this.newFiles()
    val newCommits = makeCommits(newFiles)
    val newFileNames = newCommits.flatMap(c => c.files.map(_.filename))
    
    this.copy(commitFiles = commitFiles++newFiles)
  }

}
