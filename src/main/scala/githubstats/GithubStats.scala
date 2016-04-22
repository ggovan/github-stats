package githubstats

import org.apache.spark.SparkContext

import net.liftweb.json
import net.liftweb.json.DefaultFormats

object GithubStats {
  
  def main(args: Array[String]): Unit = {
    val pathToEvents = args(0);
    val pathToCommits = args(1);

    println(pathToEvents);
    println(pathToCommits);

    val sc = new SparkContext()

    val commits = sc.wholeTextFiles(pathToCommits)
      .map(_._2)
      .flatMap { contents =>
        implicit val formats = DefaultFormats + new PayloadSerializer
        json.parse(contents)
          .extract[List[CommitResponse]]
      }

    val events = sc.wholeTextFiles(pathToEvents)
      .map(_._2)
      .flatMap { contents =>
        implicit val formats = DefaultFormats + new PayloadSerializer
        json.parse(contents)
          .extract[List[GitHubEvent[PushEventPayload]]]
      }

    // These files are not unique. The same file may be touched multiple times in different commits.
    val files = commits.flatMap { c => c.files.map(_.filename) }
    val filesString = files
      .map(fn => if (fn.contains('.')) fn.substring(fn.lastIndexOf('.')) else "No suffix")
      .countByValue().toList
      .sortBy(-_._2)
      .mkString("\n")

    println(s"There are ${events.count()} events with ${commits.count()} commits that modify ${files.count()} files.");
    println(filesString)

  }
}
