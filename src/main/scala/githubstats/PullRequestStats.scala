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

    val eventStream = ssc.receiverStream(new GitHubEventReceiver(token))
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
        implicit val formats = DefaultFormats + new PayloadSerializer
        json.Serialization.write(pr)
      }
      .foreachRDD(rdd =>
        wss.send(rdd.collect().mkString("[", ",", "]")))

    wss.start

    ssc.start()
    ssc.awaitTermination()
  }

}

/**
 * Receives events from GitHub.
 * Polls for events every two seconds.
 */
class GitHubEventReceiver(token: String) extends Receiver[GitHubEvent[Payload]](StorageLevel.MEMORY_AND_DISK_2) {

  import DefaultFormats._

  def onStart() {
    new Thread("GitHub Event Poller") {
      override def run() { poll() }
    }.start()
  }

  def onStop() = ???

  /**
   * Recursively poll the GitHub events API, parse the response and store the result.
   */
  @annotation.tailrec
  final def poll() {
    val response: HttpResponse[String] = Http(s"https://api.github.com/events?access_token=$token&per_page=2000").asString

    implicit val formats = DefaultFormats + new PayloadSerializer
    val events = json.parse(response.body)
      .extract[List[GitHubEvent[Payload]]]

    store(events.toIterator)

    Thread.sleep(2000)
    poll()
  }

}
