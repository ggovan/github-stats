package githubstats

import java.nio.file.{Files => nioFiles}

import scala.util.Try

import org.apache.spark.SparkContext
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.storage.StorageLevel

import scalaj.http._

import net.liftweb.json
import net.liftweb.json.DefaultFormats

/**
 * Spark application that polls the GitHub events API,
 */
object PullRequestStats {

  @transient var wss: WebSocketServer = _

  def main(args: Array[String]): Unit = {

    val sc = new SparkContext()
    val ssc = new StreamingContext(sc, Seconds(2))
    ssc.checkpoint(nioFiles.createTempDirectory("spark-checkpoint").toString)

    val token = sys.env("GITHUB_TOKEN")

    val eventStream = ssc.receiverStream(new GitHubReceiver[GitHubEvent[Payload]])
    wss = new WebSocketServer(5001)

    val models = eventStream.flatMap { event =>
      if (event.`type` == "PullRequestEvent")
        Some(event.asInstanceOf[GitHubEvent[PullRequestPayload]])
      else
        None
    }.map { event =>
      PRSummaryModel(event.id,
        event.payload.pull_request.base.repo.name,
        event.payload.pull_request.base.repo.language,
        event.payload.pull_request.commits,
        event.payload.pull_request.additions,
        event.payload.pull_request.deletions,
        event.payload.pull_request.changed_files)
    }

    val maxAndPrevIds = getMaxAndPreviousIds(models)

    val newModels = models.map(((), _))
      .join(maxAndPrevIds)
      .map(_._2)
      .filter { case (model, (_, maxId)) => maxId.fold(true)(_ < model.id.toLong) }
      .map { case (model, _) => model }

    newModels.map { pr =>
      implicit val formats = DefaultFormats + new GitHubEventSerializer
      json.Serialization.write(pr)
    }
      .foreachRDD(rdd =>
        wss.send(rdd.collect().mkString("[", ",", "]")))

    wss.start

    ssc.start()
    ssc.awaitTermination()
  }

  /**
   * Gets the (latest,previous_latest) ids as nested options for maximum safety.
   * It's also keyed by Unit,
   * 1) that's how the update state works, has to be done by key as there is
   *    no method for the whole rdd
   * 2) we need it for the join afterwards
   */
  def getMaxAndPreviousIds(prModels: DStream[PRSummaryModel]): DStream[(Unit,(Option[Long],Option[Long]))] =
    prModels.map(e => ((), e.id.toLong))
      .updateStateByKey(getMaxAndPrev)
  
  /**
   * Extracted from [[#getMaxAndPreviousIds]] for testing.
   */
  private[githubstats] def getMaxAndPrev(ids: Seq[Long], state: Option[(Option[Long], Option[Long])]): Option[(Option[Long], Option[Long])] = 
    Some((
      Try(ids.max).toOption
        .fold(state.fold[Option[Long]](None)(_._1))(Some(_)),
      state.fold[Option[Long]](None)(_._1)))
}
