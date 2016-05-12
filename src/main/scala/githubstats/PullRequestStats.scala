package githubstats

import org.apache.spark
import spark.SparkContext
import spark.streaming.StreamingContext
import spark.streaming.Seconds
import spark.streaming.receiver.Receiver
import spark.storage.StorageLevel

import scalaj.http._

import net.liftweb.json
import net.liftweb.json.DefaultFormats

/**
 * Spark application that polls the GitHub events API,
 */
object PullRequestStats {

  def main(args: Array[String]): Unit = {

    val sc = new SparkContext()
    val ssc = new StreamingContext(sc, Seconds(2))

    val token = sys.env("GITHUB_TOKEN")

    val eventStream = ssc.receiverStream(new GitHubReceiver[GitHubEvent[Payload]])
    val wss = new WebSocketServer(5001)

    eventStream.flatMap { event =>
      if (event.`type` == "PullRequestEvent")
        Some(event.asInstanceOf[GitHubEvent[PullRequestPayload]])
      else
        None
    }.map(event =>
      PRSummaryModel(event.id,
        event.payload.pull_request.head.repo.name,
        event.payload.pull_request.head.repo.language,
        event.payload.pull_request.commits,
        event.payload.pull_request.additions,
        event.payload.pull_request.deletions,
        event.payload.pull_request.changed_files))
      .map { pr =>
        implicit val formats = DefaultFormats + new GitHubEventSerializer
        json.Serialization.write(pr)
      }
      .foreachRDD(rdd =>
        wss.send(rdd.collect().mkString("[", ",", "]")))

    wss.start

    ssc.start()
    ssc.awaitTermination()
  }

}
